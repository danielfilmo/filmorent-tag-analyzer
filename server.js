const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
app.use(express.json());

// Config
const RESPONDIO_API_KEY = process.env.RESPONDIO_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const GOOGLE_SHEETS_URL = process.env.GOOGLE_SHEETS_URL;
const BACKFILL_TOKEN = process.env.BACKFILL_TOKEN; // v7.4: habilita GET /backfill si esta seteada
const PORT = process.env.PORT || 3000;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

// Extrae de forma robusta el texto de una respuesta de Claude. OJO: claude-sonnet-5 puede
// devolver content[0] como un bloque NO-texto (ej. razonamiento), dejando content[0].text
// undefined -> `.trim()` truena y la conversacion NO se loguea. Este helper busca el primer
// bloque de tipo 'text'; si no hay, devuelve '' (la fila se loguea igual, sin analisis).
function claudeText(resp) {
  const blocks = (resp && Array.isArray(resp.content)) ? resp.content : [];
  const t = blocks.find(b => b && b.type === 'text' && typeof b.text === 'string');
  return (t ? t.text : '').trim();
}

// v7.1: Transcribe audio from URL using OpenAI Whisper
async function transcribeAudio(audioUrl) {
  if (!openai) {
    console.log('OpenAI not configured, skipping audio transcription');
    return null;
  }

  try {
    // Download audio file to temp
    const response = await fetch(audioUrl);
    if (!response.ok) {
      console.error('Failed to download audio: ' + response.status);
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const tempFile = path.join(os.tmpdir(), 'voice_' + Date.now() + '.ogg');
    fs.writeFileSync(tempFile, buffer);

    console.log('Audio downloaded: ' + buffer.length + ' bytes, transcribing...');

    // Transcribe with Whisper
    const transcription = await openai.audio.transcriptions.create({
      file: fs.createReadStream(tempFile),
      model: 'whisper-1',
      language: 'es',
      response_format: 'text'
    });

    // Cleanup temp file
    fs.unlinkSync(tempFile);

    const text = transcription.trim();
    console.log('Transcription: "' + text + '"');
    return text;
  } catch (error) {
    console.error('Transcription error: ' + error.message);
    return null;
  }
}

// Agent mapping: userId -> {name, email, type}
const AGENT_MAP = {
  1026911: { name: 'Daniel Alonso', email: 'daniel@filmorent.com', type: 'human' },
  1027747: { name: 'Barush Villarreal', email: 'barush@filmorent.com', type: 'human' },
  1027751: { name: 'Alfredo Celedon', email: 'alfredo@filmorent.com', type: 'human' },
  1027755: { name: 'Eddy Manzano', email: 'eddy@filmorent.com', type: 'human' },
  1027757: { name: 'Diego Tovar', email: 'diego@filmorent.com', type: 'human' },
  1027820: { name: 'Suheidi Dominguez', email: 'administracion@filmorent.com', type: 'human' },
  1028000: { name: 'Filmorent Assistant', email: '', type: 'bot' }
};

// v7: Agent roles for context-aware evaluation
function getAgentRole(name) {
  const roles = {
    'Daniel Alonso': 'owner',
    'Suheidi Dominguez': 'admin',
    'Filmorent Assistant': 'bot',
    'Workflow Automatizado': 'bot'
  };
  return roles[name] || 'sales';
}

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v8.13.2', whisper: !!openai, autoSummary: true, rewards: !!BOOQABLE_API_KEY, staffGoogle: !!REWARDS_GOOGLE_CLIENT_ID, staffProtected: REWARDS_STAFF_PROTECTED, atribuciones: true }));

function extractContactId(body) {
  return (
    body?.contact?.id ||
    body?.data?.contact?.id ||
    body?.contactId ||
    body?.contact_id ||
    body?.data?.contactId ||
    body?.data?.contact_id ||
    body?.conversation?.contactId ||
    body?.data?.conversation?.contactId ||
    null
  );
}

function extractContactName(body) {
  const firstName = body?.contact?.firstName || body?.data?.contact?.firstName || '';
  const lastName = body?.contact?.lastName || body?.data?.contact?.lastName || '';
  const fullName = (firstName + ' ' + lastName).trim();
  return fullName || body?.contact_name || body?.data?.contact_name || 'Desconocido';
}

/**
 * Timestamp (ms) de un mensaje. Respond.io no expone un campo plano: la marca de
 * tiempo vive dentro de status[] (pending/sent/delivered). Tomamos la mas temprana.
 */
function msgTimestamp(msg) {
  let best = null;
  const st = msg && msg.status;
  if (Array.isArray(st)) {
    for (const s of st) {
      const t = s && s.timestamp;
      if (typeof t === 'number' && (best === null || t < best)) best = t;
    }
  }
  return best;
}

/**
 * Extract agents who ACTUALLY SENT messages.
 * Uses sender.source: "user" = human, "ai_agent" = bot
 *
 * BUGFIX 27-jul-2026: la evaluacion se dispara cuando la conversacion se CIERRA, y la
 * fila se sella con la fecha de cierre. Sin ventana, se evaluaba a CUALQUIERA que hubiera
 * escrito alguna vez en esa conversacion, aunque hubiera sido meses antes. Verificado:
 * a Barush lo calificaron estando de vacaciones (21-27 jul) por mensajes del 19-mar al
 * 14-jul. Tambien explica que TODOS bajaran a la vez y que "cierre" se desplomara parejo:
 * al cerrarse un lote de conversaciones viejas, nadie tiene un cierre decente en ellas.
 *
 * @param {number|null} sinceMs  Si se pasa, solo cuenta agentes con mensajes salientes
 *                               DENTRO de la ventana (>= sinceMs). Los mensajes sin
 *                               timestamp legible NO se descartan (no castigar por falta
 *                               de dato), pero tampoco marcan actividad reciente por si solos.
 */
function extractAgentsFromMessages(messages, sinceMs) {
  const humanAgents = new Map();
  const botAgents = new Map();
  const lastActivity = {};   // userId -> ms del ultimo mensaje

  for (const msg of messages) {
    const traffic = msg.traffic || msg.type;
    if (traffic !== 'outgoing') continue;

    const sender = msg.sender;
    if (!sender) continue;

    const uid = sender.userId;
    const source = sender.source;

    const ts = msgTimestamp(msg);
    if (uid != null && ts && (!lastActivity[uid] || ts > lastActivity[uid])) lastActivity[uid] = ts;
    // Fuera de la ventana -> ese mensaje no acredita participacion.
    if (sinceMs && ts && ts < sinceMs) continue;

    if (source === 'ai_agent') {
      if (!botAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        botAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente Virtual #' + uid,
          email: '',
          type: 'bot'
        });
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente Virtual #' + uid, email: '', type: 'bot' };
          console.log('New bot discovered: ID ' + uid);
        }
      }
    } else if (source === 'user' && uid) {
      if (!humanAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        humanAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente #' + uid,
          email: known ? known.email : '',
          type: 'human'
        });
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente #' + uid, email: '', type: 'human' };
          console.log('New agent discovered: ID ' + uid);
        }
      }
    } else if (source === 'workflow') {
      if (!botAgents.has('workflow')) {
        botAgents.set('workflow', {
          userId: 'workflow',
          name: 'Workflow Automatizado',
          email: '',
          type: 'bot'
        });
      }
    }
  }

  const stamp = a => Object.assign({}, a, {
    ultimaActividad: lastActivity[a.userId] || null
  });

  return {
    humans: Array.from(humanAgents.values()).map(stamp),
    bots: Array.from(botAgents.values()).map(stamp),
    all: [...Array.from(botAgents.values()), ...Array.from(humanAgents.values())].map(stamp)
  };
}

async function logToGoogleSheets(data) {
  if (!GOOGLE_SHEETS_URL) {
    console.log('Google Sheets URL not configured, skipping log.');
    return;
  }
  try {
    const response = await fetch(GOOGLE_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
      redirect: 'follow'
    });
    if (response.ok) {
      console.log('Logged to Google Sheets successfully.');
    } else {
      console.error('Google Sheets log failed: ' + response.status);
    }
  } catch (error) {
    console.error('Google Sheets log error: ' + error.message);
  }
}

app.post('/webhook/conversation-closed', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === WEBHOOK RECEIVED (v7) ===');
  console.log('Body:', JSON.stringify(req.body, null, 2));

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);

  if (!contactId) {
    console.error('Could not extract contact_id from payload.');
    return;
  }

  console.log('Analyzing conversation for contact: ' + contactId + ' (' + contactName + ')');

  try {
    // Fetch messages and contact info in parallel
    const [messagesResponse, contactResponse] = await Promise.all([
      fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=50',
        {
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          }
        }
      ),
      fetch(
        'https://api.respond.io/v2/contact/id:' + contactId,
        {
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          }
        }
      )
    ]);

    if (!messagesResponse.ok) {
      const errorText = await messagesResponse.text();
      console.error('Respond.io messages API error: ' + messagesResponse.status + ' - ' + errorText);
      return;
    }

    const messagesData = await messagesResponse.json();
    const messages = messagesData.data || messagesData.items || [];

    if (messages.length === 0) {
      console.log('No messages found for this contact');
      return;
    }

    // Get assignee (for reference only)
    let assignee = null;
    if (contactResponse.ok) {
      const contactData = await contactResponse.json();
      assignee = contactData.assignee || null;
    }

    // Extract agents who ACTUALLY sent messages DENTRO de la ventana de evaluacion.
    // Sin esta ventana se calificaba a gente por mensajes de meses atras (ver BUGFIX en
    // extractAgentsFromMessages). EVAL_WINDOW_DAYS=0 desactiva el filtro.
    const evalWindowDays = Number(process.env.EVAL_WINDOW_DAYS || 7);
    const sinceMs = evalWindowDays > 0 ? Date.now() - evalWindowDays * 86400000 : null;
    const { humans, bots, all: allAgents } = extractAgentsFromMessages(messages, sinceMs);
    const humanNames = humans.map(a => a.name);
    const botNames = bots.map(a => a.name);
    const allNames = allAgents.map(a => a.name).join(', ') || 'Sin agente';

    console.log('Agents detected - Humans: ' + humanNames.join(', ') + ' | Bots: ' + botNames.join(', '));

    // v7.1: Format messages including internal notes and voice transcriptions
    const formattedMessagesArray = await Promise.all(messages.map(async (msg) => {
      const traffic = msg.traffic || msg.type;
      const messageObj = msg.message || {};
      const attachment = messageObj.attachment || {};

      // v7.1: Check if this is a voice/audio message
      const isAudio = (
        messageObj.type === 'attachment' &&
        attachment.type === 'audio' &&
        attachment.url
      );

      // v7: Detect internal notes/comments
      if (traffic === 'internal' || msg.internal === true || msg.messageType === 'internal') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const known = uid ? AGENT_MAP[uid] : null;
        const senderName = known ? known.name : (sender?.name || 'Equipo');
        const text = msg.text || messageObj.text || msg.body || '[nota sin texto]';
        return 'NOTA INTERNA de ' + senderName + ': ' + text;
      }

      if (traffic === 'incoming') {
        // v7.1: Transcribe voice messages from clients
        if (isAudio) {
          const transcription = await transcribeAudio(attachment.url);
          if (transcription) {
            return 'CLIENTE [mensaje de voz]: ' + transcription;
          }
          return 'CLIENTE: [mensaje de voz - no se pudo transcribir]';
        }
        const text = msg.text || messageObj.text || msg.body || '[media/attachment]';
        return 'CLIENTE: ' + text;
      } else if (traffic === 'outgoing') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const source = sender?.source;
        let label = 'AGENTE';

        if (source === 'ai_agent') {
          const known = AGENT_MAP[uid];
          label = '[BOT] ' + (known ? known.name : 'Agente Virtual');
        } else if (source === 'user' && uid) {
          const known = AGENT_MAP[uid];
          label = known ? known.name : 'Agente #' + uid;
        } else if (source === 'workflow') {
          label = '[WORKFLOW]';
        }

        // v7.1: Transcribe voice messages from agents too
        if (isAudio) {
          const transcription = await transcribeAudio(attachment.url);
          if (transcription) {
            return label + ' [mensaje de voz]: ' + transcription;
          }
          return label + ': [mensaje de voz - no se pudo transcribir]';
        }
        const text = msg.text || messageObj.text || msg.body || '[media/attachment]';
        return label + ': ' + text;
      }

      // Fallback for any other message type
      const text = msg.text || messageObj.text || msg.body || '';
      if (text) return '[SISTEMA]: ' + text;
      return null;
    }));
    const formattedMessages = formattedMessagesArray.filter(m => m !== null).join('\n');

    const channel = messages[0]?.channelType || messages[0]?.channel || 'desconocido';
    const link = 'https://app.respond.io/space/379868/inbox/' + contactId;

    const hasBotAgent = bots.length > 0;
    const hasHumanAgent = humans.length > 0;

    // v7: Build conversation identifier
    const conversacionId = contactName + ' - ' + channel + ' (#' + contactId + ')';

    // v7: Build agent roles info
    const agentRolesInfo = humans.map(a => {
      const role = getAgentRole(a.name);
      const roleDesc = {
        'owner': 'due√±o del negocio',
        'admin': 'administraci√≥n (facturaci√≥n, cobranza, log√≠stica)',
        'sales': 'ventas y atenci√≥n al cliente'
      };
      return a.name + ' (' + (roleDesc[role] || role) + ')';
    }).join(', ');

    // v7: Build evaluation instructions per agent
    let humanEvalInstructions = '';
    if (hasHumanAgent) {
      const agentEntries = humanNames.map(name => {
        const role = getAgentRole(name);
        return `  {
    "nombre_agente": "${name}",
    "rol": "${role}",
    "atencion_cliente": 8,
    "conocimiento_solucion": 7,
    "proactividad": 8,
    "cierre_resultado": 7,
    "calificacion_general": 7.5,
    "feedback": "Maximo 2 oraciones: 1 cosa bien + 1 a mejorar (si aplica). Si hizo bien su trabajo, solo reconocerlo."
  }`;
      }).join(',\n');

      humanEvalInstructions = `
"evaluaciones_individuales": [
${agentEntries}
]`;
    }

    let botEvalInstructions = '';
    if (hasBotAgent) {
      botEvalInstructions = `
"evaluacion_bot": {
  "precision_respuestas": 8,
  "manejo_consulta": 7,
  "transicion_humano": 9,
  "tono_comunicacion": 8,
  "calificacion_general": 8,
  "feedback_bot": "Que hizo bien y que deberia mejorar el bot.",
  "mejoras_sugeridas": ["Mejora 1", "Mejora 2"]
}`;
    }

    // v7: COMPLETELY REWRITTEN PROMPT
    const analysisPrompt = `Eres el evaluador de servicio al cliente de Filmorent, un negocio de RENTA de equipo de cine y fotografia en Monterrey, Mexico.

=== PASO 1: ENTENDER LA CONVERSACION COMPLETA ===

Lee TODA la conversacion de principio a fin. Entiende:
- Que necesitaba el cliente
- Como respondio el equipo EN CONJUNTO
- Cual fue el resultado final
- Las NOTAS INTERNAS son instrucciones del due√±o (Daniel Alonso) al equipo. Seguirlas es CORRECTO.

=== PASO 2: REGLAS CRITICAS DE EVALUACION ===

REGLA 1 - TRABAJO EN EQUIPO: Los agentes trabajan como EQUIPO. Si un agente solo envio un mensaje de cierre cortes o de seguimiento, eso es POSITIVO y demuestra trabajo en equipo. NO penalizar porque "su participacion fue limitada" - cada mensaje cuenta.

REGLA 2 - RAPIDEZ ES BUENA: Enviar cotizacion o informacion rapido es BUENO para el negocio. NUNCA penalizar por "enviar cotizacion antes de explicar" o "no dar contexto previo". La rapidez cierra rentas.

REGLA 3 - ROLES DIFERENTES: Cada agente tiene un ROL diferente:
${agentRolesInfo}
- Agentes de ADMIN: Evaluar en facturacion, cobranza, logistica. NO penalizar por "no conocer equipos".
- Agentes de VENTAS: Evaluar en atencion, conocimiento de equipos, cierre de rentas.
- El DUE√ëO: Generalmente da instrucciones internas, no evaluarlo a menos que interactue con el cliente.

REGLA 4 - NOTAS INTERNAS: Los mensajes marcados "NOTA INTERNA" son instrucciones del due√±o al equipo. Si un agente sigue una instruccion interna (ej: "ofrecele la ZVE10"), eso es CORRECTO. No penalizar por "introducir informacion no solicitada" cuando fue una instruccion.

REGLA 5 - ENFOCARSE EN LO IMPORTANTE: Evalua lo que REALMENTE importa para el negocio:
- Se atendio bien al cliente?
- Se respondieron TODAS sus preguntas?
- Se busco resolver su necesidad?
- Se contribuyo a concretar la renta?
NO buscar defectos artificiales. Si el agente hizo bien su trabajo, di que lo hizo bien.

REGLA 6 - FEEDBACK UTIL: El feedback debe ser ACCIONABLE y ENFOCADO. Maximo 2 oraciones: 1 cosa positiva + 1 area de mejora (SOLO si realmente hay algo importante que mejorar). Si el agente hizo bien su trabajo, no inventes criticas.

=== DATOS DE LA CONVERSACION ===

Conversacion: ${conversacionId}
Canal: ${channel}
Agentes humanos: ${humanNames.join(', ') || 'Ninguno'}
Roles: ${agentRolesInfo || 'N/A'}
Bot: ${botNames.join(', ') || 'Ninguno'}

=== CONVERSACION ===
${formattedMessages}

=== PASO 3: EVALUAR ===

Criterios por agente humano (1-10):
1. atencion_cliente: Cordialidad, profesionalismo, respondio todas las preguntas del cliente? (Adaptado al ROL del agente)
2. conocimiento_solucion: Demostro conocimiento relevante a su rol y busco soluciones? (Para ventas: equipos. Para admin: procesos administrativos)
3. proactividad: Fue rapido, ofrecio alternativas, dio seguimiento, tomo iniciativa?
4. cierre_resultado: Contribuyo a concretar la renta o resolver la necesidad del cliente?

TAGS A EVALUAR:
1. "consulta-compra" - Cliente pregunta por COMPRAR equipo (Filmorent solo renta).
2. "equipo-no-disponible" - Equipo no disponible (no existe O ya rentado).
3. "incidencia" - Problema, queja, equipo da√±ado, entrega tarde.
4. "renta-perdida" - Cliente queria rentar pero NO se concreto. Causa: "precio", "sin_respuesta_cliente", "tardanza_respuesta", "fechas", "ubicacion", "otro".

Responde UNICAMENTE con JSON valido (sin markdown, sin backticks, solo JSON puro):
{
  "conversacion_id": "${conversacionId}",
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen de 3-5 oraciones: que pidio el cliente, que paso, y cual fue el RESULTADO FINAL.",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  ${hasHumanAgent ? humanEvalInstructions + ',' : '"evaluaciones_individuales": [],'}
  ${hasBotAgent ? botEvalInstructions : '"evaluacion_bot": null'}
}`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 4000,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeText(claudeResponse);
    console.log('Claude analysis: ' + analysisText);

    let tagsToApply = [];
    let causaRentaPerdida = null;
    let resumen = '';
    let equipos = [];
    let evaluacionesIndividuales = [];
    let evaluacionBot = null;
    let resultado = '';
    let parsedConversacionId = conversacionId;

    try {
      let cleanJson = analysisText;
      if (cleanJson.startsWith('```')) {
        cleanJson = cleanJson.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      }
      const parsed = JSON.parse(cleanJson);
      tagsToApply = parsed.tags || [];
      causaRentaPerdida = parsed.causa_renta_perdida || null;
      resumen = parsed.resumen || '';
      equipos = parsed.equipos_solicitados || [];
      evaluacionesIndividuales = parsed.evaluaciones_individuales || [];
      evaluacionBot = parsed.evaluacion_bot || null;
      resultado = parsed.resultado || '';
      parsedConversacionId = parsed.conversacion_id || conversacionId;
    } catch (e) {
      console.log('JSON parse error: ' + e.message);
      const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'renta-perdida'];
      validTags.forEach(tag => {
        if (analysisText.toLowerCase().includes(tag)) {
          tagsToApply.push(tag);
        }
      });
      resumen = 'Error parsing JSON';
    }

    const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'renta-perdida'];
    tagsToApply = tagsToApply.filter(tag => validTags.includes(tag));

    if (tagsToApply.includes('renta-perdida') && causaRentaPerdida) {
      tagsToApply.push('renta-perdida:' + causaRentaPerdida);
    }

    // Apply tags to Respond.io
    if (tagsToApply.length > 0) {
      const tagResponse = await fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/tag',
        {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ tags: tagsToApply })
        }
      );
      if (!tagResponse.ok) {
        const errorText = await tagResponse.text();
        console.error('Failed to apply tags: ' + tagResponse.status + ' - ' + errorText);
      } else {
        console.log('Tags applied: ' + tagsToApply.join(', '));
      }
    }

    // v7: Add role to each evaluation
    evaluacionesIndividuales = evaluacionesIndividuales.map(ev => ({
      ...ev,
      rol: ev.rol || getAgentRole(ev.nombre_agente)
    }));

    // Log to Google Sheets - v7 format
    await logToGoogleSheets({
      version: 'v7.1',
      fecha: new Date().toISOString(),
      contactId: contactId,
      nombre: contactName,
      conversacion_id: parsedConversacionId,
      tags: tagsToApply.join(', '),
      causa_renta_perdida: causaRentaPerdida || '',
      num_mensajes: messages.length,
      canal: channel,
      resumen: resumen,
      link_conversacion: link,
      conversacion_completa: formattedMessages.substring(0, 45000),
      resultado: resultado,
      equipos_solicitados: equipos,
      // v7: Individual evaluations with roles and 4 criteria
      evaluaciones_individuales: evaluacionesIndividuales,
      evaluacion_bot: evaluacionBot,
      agentes_humanos: humans.map(a => ({ nombre: a.name, email: a.email, userId: a.userId })),
      agentes_bot: bots.map(a => ({ nombre: a.name, userId: a.userId })),
      agentes_todos: allNames,
      assignee: assignee ? (assignee.firstName + ' ' + (assignee.lastName || '')).trim() : 'Sin asignar'
    });

    const califBot = evaluacionBot ? evaluacionBot.calificacion_general : 'N/A';
    const califHumanos = evaluacionesIndividuales.map(e => e.nombre_agente + '=' + e.calificacion_general).join(', ');
    console.log('=== DONE: contact=' + contactId + ', calif_humanos=[' + califHumanos + '], calif_bot=' + califBot + ' ===\n');

  } catch (error) {
    console.error('Error:', error.message);
  }
});

// ============================================================
// v7.2: AUTO-SUMMARY on Conversation Opened
// When a conversation reopens, generate an AI summary of ALL
// previous messages and post it as an internal comment so
// the agent has immediate context.
// ============================================================

app.post('/webhook/conversation-opened', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === CONVERSATION OPENED (v7.2) ===');

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);

  if (!contactId) {
    console.error('conversation-opened: Could not extract contact_id');
    return;
  }

  console.log('Generating summary for: ' + contactId + ' (' + contactName + ')');

  try {
    // Fetch ALL messages for this contact (up to 200 for full context)
    const messagesResponse = await fetch(
      'https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=200',
      {
        headers: {
          'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!messagesResponse.ok) {
      console.error('Failed to fetch messages: ' + messagesResponse.status);
      return;
    }

    const messagesData = await messagesResponse.json();
    const messages = messagesData.data || messagesData.items || [];

    // Only generate summary if there are enough previous messages
    if (messages.length < 3) {
      console.log('Too few messages (' + messages.length + '), skipping summary');
      return;
    }

    // Format messages (simplified - no audio transcription for speed)
    const formattedMessages = messages.map(msg => {
      const traffic = msg.traffic || msg.type;
      const messageObj = msg.message || {};
      const text = msg.text || messageObj.text || msg.body || '';

      if (traffic === 'internal' || msg.internal === true) {
        const sender = msg.sender;
        const uid = sender?.userId;
        const known = uid ? AGENT_MAP[uid] : null;
        const senderName = known ? known.name : 'Equipo';
        return 'NOTA INTERNA de ' + senderName + ': ' + (text || '[nota]');
      }

      if (traffic === 'incoming') {
        return 'CLIENTE: ' + (text || '[media]');
      } else if (traffic === 'outgoing') {
        const sender = msg.sender;
        const uid = sender?.userId;
        const source = sender?.source;
        let label = 'AGENTE';

        if (source === 'ai_agent') {
          const known = AGENT_MAP[uid];
          label = '[BOT] ' + (known ? known.name : 'Bot');
        } else if (source === 'user' && uid) {
          const known = AGENT_MAP[uid];
          label = known ? known.name : 'Agente';
        } else if (source === 'workflow') {
          label = '[WORKFLOW]';
        }
        return label + ': ' + (text || '[media]');
      }
      return null;
    }).filter(m => m !== null).join('\n');

    if (!formattedMessages || formattedMessages.length < 50) {
      console.log('Not enough text content to summarize');
      return;
    }

    // Generate summary with Claude
    const summaryPrompt = `Eres asistente de Filmorent (renta de equipo de cine/foto en Monterrey).

Genera un resumen BREVE y UTIL para el agente que va a atender a este cliente que vuelve a escribir. El agente necesita saber rapidamente:

1. **Quien es**: Nombre del cliente y canal de comunicacion
2. **Que ha pedido antes**: Equipos solicitados, fechas, proyectos mencionados
3. **Estado actual**: Se concreto alguna renta? Quedo algo pendiente? Hubo algun problema?
4. **Datos clave**: Precios cotizados, acuerdos hechos, condiciones especiales
5. **Lo mas reciente**: Que paso en la ultima conversacion

IMPORTANTE:
- Maximo 8-10 lineas
- Se directo y practico, esto es para que el agente sepa que paso SIN tener que leer todo
- Si hubo multiples conversaciones/ciclos, resume TODOS, no solo el ultimo
- Usa formato simple con vi√±etas
- Escribe en espa√±ol

=== HISTORIAL COMPLETO DEL CLIENTE: ${contactName} ===
${formattedMessages}

=== FIN DEL HISTORIAL ===

Genera el resumen ahora:`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 1000,
      messages: [{ role: 'user', content: summaryPrompt }]
    });

    const summary = claudeText(claudeResponse);
    console.log('Summary generated: ' + summary.substring(0, 200) + '...');

    // Post summary as internal comment via Respond.io API
    const commentResponse = await fetch(
      'https://api.respond.io/v2/contact/id:' + contactId + '/comment',
      {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          text: 'ü§ñ RESUMEN AUTOMATICO (IA)\n‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ\n' + summary + '\n‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ‚îÅ\nüìù Resumen generado al reabrir conversacion'
        })
      }
    );

    if (commentResponse.ok) {
      console.log('Summary posted as internal comment for contact ' + contactId);
    } else {
      const errorText = await commentResponse.text();
      console.error('Failed to post comment: ' + commentResponse.status + ' - ' + errorText);
    }

  } catch (error) {
    console.error('conversation-opened error: ' + error.message);
  }
});


// ============================================================
// v7.3: CALL ENDED ‚Äî analiza la transcripcion de una llamada
// (Respond.io Voice AI / llamadas) y aplica tags. Si no hay
// transcript (llamada perdida o sin grabacion), se omite.
// ============================================================

function extractCallTranscript(body) {
  return (
    body?.call?.transcript ||
    body?.data?.call?.transcript ||
    body?.transcript ||
    body?.data?.transcript ||
    null
  );
}

function extractCallSummary(body) {
  return (
    body?.call?.summary ||
    body?.data?.call?.summary ||
    body?.summary ||
    body?.data?.summary ||
    body?.call?.aiSummary ||
    body?.data?.call?.aiSummary ||
    null
  );
}

function extractCallMeta(body) {
  const call = body?.call || body?.data?.call || body || {};
  return {
    callId: call?.id || call?.callId || null,
    duration: call?.duration || call?.durationSeconds || null,
    status: call?.status || null,
    direction: call?.direction || null
  };
}

app.post('/webhook/call-ended', async (req, res) => {
  console.log('\n[' + new Date().toISOString() + '] === CALL ENDED (v7.3) ===');
  console.log('Body:', JSON.stringify(req.body, null, 2));

  res.json({ received: true });

  const contactId = extractContactId(req.body);
  const contactName = extractContactName(req.body);
  const transcript = extractCallTranscript(req.body);
  const summary = extractCallSummary(req.body);
  const meta = extractCallMeta(req.body);

  console.log('Call meta: ' + JSON.stringify(meta) + ', contact=' + contactId + ' (' + contactName + ')');

  if (!contactId) {
    console.error('call-ended: Could not extract contact_id');
    return;
  }

  if (!transcript) {
    console.log('call-ended: No transcript for call ' + meta.callId + ' (llamada perdida, sin grabacion o en curso). Skipping.');
    return;
  }

  try {
    const summarySection = summary ? ('\n\nRESUMEN AI:\n' + summary) : '';

    const analysisPrompt = 'Analiza la siguiente transcripcion de una LLAMADA telefonica entrante a Filmorent (renta de equipo audiovisual en Monterrey). Determina si aplica alguno de estos tags:\n\n' +
      '1. "consulta-compra" - El cliente pregunto por COMPRAR equipo (no rentar). Filmorent solo renta, no vende.\n' +
      '2. "equipo-no-disponible" - El cliente pregunto por equipo que probablemente NO esta en el catalogo de renta.\n' +
      '3. "incidencia" - El cliente reporto un problema, queja, equipo danado, entrega tarde, cobro incorrecto o situacion negativa.\n' +
      '4. "llamada-cotizacion" - La llamada fue una cotizacion exitosa donde el cliente dejo datos (nombre, equipo, fechas, contacto) y se le ofrecio mandar cotizacion por WhatsApp.\n\n' +
      'TRANSCRIPCION:\n' + transcript + summarySection + '\n\n' +
      'Responde UNICAMENTE con un JSON valido en este formato exacto, sin texto adicional:\n' +
      '{"tags": ["tag1", "tag2"]}\n\n' +
      'Si no aplica ningun tag, responde: {"tags": []}\n' +
      'Solo incluye tags que CLARAMENTE apliquen basado en la llamada.';

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-5',
      max_tokens: 150,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeText(claudeResponse);
    console.log('Claude call analysis: ' + analysisText);

    let tagsToApply = [];
    try {
      let cleanJson = analysisText;
      if (cleanJson.startsWith('```')) {
        cleanJson = cleanJson.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      }
      const parsed = JSON.parse(cleanJson);
      tagsToApply = parsed.tags || [];
    } catch (e) {
      const vt = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'llamada-cotizacion'];
      vt.forEach(function (tag) {
        if (analysisText.toLowerCase().includes(tag)) tagsToApply.push(tag);
      });
    }

    const validTags = ['consulta-compra', 'equipo-no-disponible', 'incidencia', 'llamada-cotizacion'];
    tagsToApply = tagsToApply.filter(function (tag) { return validTags.includes(tag); });

    if (tagsToApply.length > 0) {
      const tagResponse = await fetch(
        'https://api.respond.io/v2/contact/id:' + contactId + '/tag',
        {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + RESPONDIO_API_KEY,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ tags: tagsToApply })
        }
      );
      if (!tagResponse.ok) {
        const errorText = await tagResponse.text();
        console.error('call-ended: Failed to apply tags: ' + tagResponse.status + ' - ' + errorText);
      } else {
        console.log('call-ended: Tags applied: ' + tagsToApply.join(', '));
      }
    } else {
      console.log('call-ended: No tags to apply');
    }

    console.log('=== DONE CALL: contact=' + contactId + ', call=' + meta.callId + ', tags=' + JSON.stringify(tagsToApply) + ' ===\n');
  } catch (error) {
    console.error('call-ended error: ' + error.message);
  }
});


// ‚îÄ‚îÄ v7.4 BACKFILL (30-jul-2026) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// Rellena huecos del Log de Conversaciones (ej. 15-jun‚Üí13-jul 2026, cuando el
// modelo Claude descontinuado rompia el analisis). READ-ONLY: solo ENUMERA los
// contactId cuyo "ultimo mensaje entrante" cae en el rango, via POST /contact/list
// (API v2). No reprocesa ni escribe nada aqui ‚Äî de eso se encarga el script local
// webhook-server/backfill.py, que es resumible (no duplica filas al re-correr).
//
// Protegido: requiere la env BACKFILL_TOKEN. Si NO esta seteada, el endpoint esta
// DESHABILITADO (403). Setea BACKFILL_TOKEN en Render para habilitarlo, corre el
// backfill, y luego puedes quitarla.
//   GET /backfill?token=XXX&from=2026-06-15&to=2026-07-13
//   opcionales: field, valueFormat (ms|iso|s|datetime), probe=1, max (tope, def 3000)
const BACKFILL_TZ = 'America/Monterrey'; // UTC-6 fijo (Monterrey no observa DST)
const BACKFILL_FIELD_CANDIDATES = [
  'lastIncomingMessageTime', 'lastIncomingMessage', 'latestIncomingMessageTime',
  'lastInboundMessageTime', 'lastContactTime', 'lastMessageTime', 'lastInteraction'
];
const BACKFILL_FMT_CANDIDATES = ['ms', 'iso', 's', 'datetime'];

// pagination.next puede ser un cursor pelon o una URL con ?cursorId=... ‚Äî extrae el cursor.
function backfillNextCursor(next) {
  if (!next) return null;
  const s = String(next);
  const m = s.match(/cursorId=([^&]+)/);
  return m ? decodeURIComponent(m[1]) : s;
}

function backfillFmtValue(dateStr, which, fmt) {
  // dateStr = 'YYYY-MM-DD'; which='from' => inicio de dia, 'to' => fin de dia (MTY, UTC-6)
  if (fmt === 'datetime') return which === 'to' ? dateStr + ' 23:59' : dateStr + ' 00:00';
  const iso = (which === 'to' ? dateStr + 'T23:59:59' : dateStr + 'T00:00:00') + '-06:00';
  const d = new Date(iso);
  if (fmt === 'ms') return String(d.getTime());
  if (fmt === 's') return String(Math.floor(d.getTime() / 1000));
  return d.toISOString(); // iso
}

async function backfillListPage(field, value, cursorId, limit) {
  let url = 'https://api.respond.io/v2/contact/list?limit=' + (limit || 100);
  if (cursorId) url += '&cursorId=' + encodeURIComponent(cursorId);
  const body = {
    search: '', // la API la requiere presente aunque sea vacia (400 si falta)
    timezone: BACKFILL_TZ,
    filter: { $and: [ { category: 'contactField', field: field, operator: 'isTimestampBetween', value: value } ] }
  };
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  const text = await resp.text();
  let json = null; try { json = JSON.parse(text); } catch (e) {}
  const items = (json && (json.items || json.data)) || [];
  const next = json && json.pagination && json.pagination.next;
  return { status: resp.status, items: Array.isArray(items) ? items : [], next: next || null, text: text };
}

// Descubre la combinacion (field, valueFormat) que la API acepta (status 200 con items>0).
async function backfillDiscover(from, to, forceField, forceFmt) {
  const fields = (forceField ? [forceField] : BACKFILL_FIELD_CANDIDATES);
  const fmts = (forceFmt ? [forceFmt] : BACKFILL_FMT_CANDIDATES);
  const log = [];
  let fallback = null; // combo que dio 200 aunque con 0 items
  for (const field of fields) {
    for (const fmt of fmts) {
      const value = { from: backfillFmtValue(from, 'from', fmt), to: backfillFmtValue(to, 'to', fmt) };
      try {
        const r = await backfillListPage(field, value, null, 1);
        log.push({ field: field, fmt: fmt, status: r.status, items: r.items.length,
          err: r.status >= 400 ? String(r.text || '').slice(0, 250) : undefined });
        if (r.status === 200 && r.items.length > 0) return { field: field, fmt: fmt, value: value, log: log };
        if (r.status === 200 && !fallback) fallback = { field: field, fmt: fmt, value: value };
      } catch (e) { log.push({ field: field, fmt: fmt, error: e.message }); }
    }
  }
  if (fallback) return { field: fallback.field, fmt: fallback.fmt, value: fallback.value, log: log, empty: true };
  return { field: null, fmt: null, value: null, log: log };
}

app.get('/backfill', async (req, res) => {
  if (!BACKFILL_TOKEN) return res.status(403).json({ error: 'backfill deshabilitado: setea BACKFILL_TOKEN en el env para habilitarlo' });
  if (req.query.token !== BACKFILL_TOKEN) return res.status(401).json({ error: 'token invalido' });
  const from = req.query.from, to = req.query.to;
  if (!from || !to) return res.status(400).json({ error: 'faltan from y to (YYYY-MM-DD)' });

  // DIAGNOSTICO: ?dump=1 -> lista 1 contacto (por createdAt, que si funciona) y devuelve
  // el objeto completo para descubrir los nombres de campo reales (ej. el de ultimo mensaje).
  if (req.query.dump === '1') {
    try {
      const val = { from: backfillFmtValue(from, 'from', 'datetime'), to: backfillFmtValue(to, 'to', 'datetime') };
      const r = await backfillListPage('createdAt', val, null, 1);
      const item = (r.items && r.items[0]) || null;
      let full = null;
      if (item && item.id) {
        const gr = await fetch('https://api.respond.io/v2/contact/id:' + item.id, {
          headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
        });
        try { full = await gr.json(); } catch (e) { full = null; }
      }
      return res.json({
        dump: true, listStatus: r.status,
        listItemKeys: item ? Object.keys(item) : null,
        listItem: item,
        fullContactKeys: full ? Object.keys(full) : null,
        fullContact: full
      });
    } catch (e) { return res.status(500).json({ dumpError: e.message }); }
  }

  try {
    const disc = await backfillDiscover(from, to, req.query.field, req.query.valueFormat);
    if (req.query.probe === '1') return res.json({ probe: true, chosen: { field: disc.field, fmt: disc.fmt }, log: disc.log, value: disc.value });
    if (!disc.field) return res.status(502).json({ error: 'ningun field/formato aceptado por la API', log: disc.log });

    const max = parseInt(req.query.max || '3000', 10);
    const ids = [];
    let cursorId = null, pages = 0, lastStatus = null;
    while (ids.length < max && pages < 60) {
      const r = await backfillListPage(disc.field, disc.value, cursorId, 100);
      lastStatus = r.status;
      if (r.status >= 400) break;
      for (const it of r.items) { if (it && it.id) ids.push(it.id); }
      pages++;
      if (!r.next || r.items.length === 0) break;
      cursorId = backfillNextCursor(r.next);
    }
    const uniqueIds = Array.from(new Set(ids));
    return res.json({
      ok: true, field: disc.field, valueFormat: disc.fmt, value: disc.value,
      windowEmpty: !!disc.empty, lastStatus: lastStatus, pages: pages,
      count: uniqueIds.length, sample: uniqueIds.slice(0, 10), ids: uniqueIds
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// ‚îÄ‚îÄ v7.5 BACKFILL SCAN (para el hueco COMPLETO, incl. clientes recurrentes) ‚îÄ‚îÄ
// La API publica solo deja filtrar contactos por createdAt (no por "ultimo mensaje").
// Para atrapar tambien a los recurrentes activos en la ventana, este endpoint pagina
// TODOS los contactos (por createdAt <= to) y para cada uno mira el timestamp de su
// ultimo mensaje; marca inWindow si cae en [from,to]. Lo maneja el backfill.py local
// (paginado + resumible). READ-ONLY: no escribe ni taggea.
//   GET /backfill/scan?token=..&from=YYYY-MM-DD&to=YYYY-MM-DD&cursorId=&limit=40
//   -> { results:[{id, ts, inWindow}], nextCursorId, scanned, sampleMsgKeys, sampleMsg }

// extrae el timestamp (epoch ms) del objeto mensaje. OJO: los mensajes de Respond.io v2
// NO traen campo de fecha explicito; el timestamp va codificado en messageId (epoch en
// MICROSEGUNDOS, ej. 1784052692000000 -> /1000 = ms). Normaliza por magnitud por si acaso.
function backfillMsgTs(msg) {
  if (!msg) return null;
  const cands = [msg.messageId, msg.timestamp, msg.createdAt, msg.created_at, msg.time,
    msg.messageTime, msg.sentAt, msg.sent_at, msg.date];
  for (let v of cands) {
    if (v == null) continue;
    let n = null;
    if (typeof v === 'number') n = v;
    else if (typeof v === 'string' && /^\d+$/.test(v)) n = Number(v);
    else if (typeof v === 'string') { const t = Date.parse(v); if (!isNaN(t)) return t; continue; }
    if (n == null || !isFinite(n)) continue;
    if (n >= 1e15) return Math.floor(n / 1000); // microsegundos -> ms
    if (n >= 1e12) return n;                     // ya en ms
    if (n >= 1e9) return n * 1000;              // segundos -> ms
  }
  return null;
}

async function backfillFetchMessages(contactId, limit, attempt) {
  const r = await fetch('https://api.respond.io/v2/contact/id:' + contactId + '/message/list?limit=' + (limit || 3), {
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return backfillFetchMessages(contactId, limit, (attempt || 0) + 1);
  }
  if (!r.ok) return { status: r.status, messages: [] };
  const j = await r.json().catch(() => null);
  return { status: r.status, messages: (j && (j.data || j.items)) || [] };
}

app.get('/backfill/scan', async (req, res) => {
  if (!BACKFILL_TOKEN) return res.status(403).json({ error: 'backfill deshabilitado: setea BACKFILL_TOKEN' });
  if (req.query.token !== BACKFILL_TOKEN) return res.status(401).json({ error: 'token invalido' });
  const from = req.query.from, to = req.query.to;
  if (!from || !to) return res.status(400).json({ error: 'faltan from y to (YYYY-MM-DD)' });

  const limit = Math.min(parseInt(req.query.limit || '40', 10), 60);
  const cursorId = req.query.cursorId || null;
  const fromMs = new Date(from + 'T00:00:00-06:00').getTime();
  const toMs = new Date(to + 'T23:59:59-06:00').getTime();

  try {
    // 1) una pagina de contactos (todos los creados hasta 'to', datetime que ya sabemos que sirve)
    const listVal = { from: backfillFmtValue('2015-01-01', 'from', 'datetime'), to: backfillFmtValue(to, 'to', 'datetime') };
    const page = await backfillListPage('createdAt', listVal, cursorId, limit);
    if (page.status >= 400) return res.status(502).json({ error: 'list fallo', status: page.status, body: (page.text || '').slice(0, 300) });

    const results = [];
    let sampleMsg = null, sampleMsgKeys = null;
    for (let i = 0; i < page.items.length; i++) {
      const it = page.items[i];
      if (!it || !it.id) continue;
      const mr = await backfillFetchMessages(it.id, 3, 0);
      const newest = mr.messages && mr.messages[0] ? mr.messages[0] : null;
      if (i === 0 && newest) { sampleMsg = newest; sampleMsgKeys = Object.keys(newest); }
      const ts = backfillMsgTs(newest);
      const inWindow = ts != null && ts >= fromMs && ts <= toMs;
      results.push({ id: it.id, ts: ts, inWindow: inWindow, msgStatus: mr.status, mid: newest ? newest.messageId : null });
      await new Promise(z => setTimeout(z, 120)); // suave con el rate limit
    }
    return res.json({
      ok: true, scanned: page.items.length,
      nextCursorId: page.next ? backfillNextCursor(page.next) : null,
      matched: results.filter(r => r.inWindow).map(r => r.id),
      results: results,
      sampleMsgKeys: sampleMsgKeys, sampleMsg: sampleMsg
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});



// ============================================================
// v8.0 FILMORENT REWARDS (Pilar 6) — 22-jul-2026
// Proxy server-side de Booqable para el portal rewards.filmorent.com.
// La key de Booqable vive en env (BOOQABLE_API_KEY) — NUNCA en el frontend.
//
//   GET  /rewards/member?email=...  -> puntos, tier-data, historial (calculado
//                                      de Booqable EN VIVO, excluyendo lineas
//                                      ELSEPC) menos canjes del Ledger
//   POST /rewards/redeem            -> valida saldo y registra canje en Ledger
//   POST /rewards/scan              -> resuelve QR de miembro y registra scan
//   GET  /rewards/folio?f=RWD-...   -> staff: estado de un folio de canje (Ledger)
//   POST /rewards/folio/aplicar     -> staff: marca un folio como aplicado a orden
//
// Persistencia: Google Sheet "Rewards Ledger" via Apps Script (doPost para
// escribir canjes/scans, doGet para leer canjes por customer) — mismo patron
// logToGoogleSheets que el Log de Conversaciones. Env: REWARDS_SHEETS_URL.
//
// Reglas de negocio (pilar6-rewards/README.md):
//   - 1 pt / $100 MXN. Base = grand_total_in_cents (sin IVA) de ordenes con
//     status distinto de draft/concept/canceled — verificado que la suma
//     coincide EXACTO con customer.revenue_in_cents.
//   - La linea del producto ELSEPC no genera puntos (subrenta: 90% no es
//     ingreso nuestro). El precio de linea viene CON IVA, asi que se convierte
//     a base sin IVA con el ratio grand_total/grand_total_with_tax de su orden.
//   - Canje = "crédito calibrado" (decisión F0, 23-jul-2026): crédito en pesos
//     por nivel de puntos, con doble tope y dedupe. Ver rewardsCatalogFor().
//   - QR determinstico del customer_id, formato FLM-XX-YYYY-XXNX. Hash FNV-1a
//     32-bit (el hash del prototipo — suma de charCodes — solo producia ~30k
//     valores => colisiones casi seguras entre ~2k miembros).
// Deploys automatizados con token fine-grained desde 27-jul-2026.
// ============================================================

const BOOQABLE_API_KEY = process.env.BOOQABLE_API_KEY;
const BOOQABLE_BASE = process.env.BOOQABLE_BASE || 'https://filmorent-sa-de-cv.booqable.com/api/4';
const REWARDS_SHEETS_URL = process.env.REWARDS_SHEETS_URL; // Apps Script del Rewards Ledger
const REWARDS_STAFF_PIN = process.env.REWARDS_STAFF_PIN;   // PIN genérico (legacy/respaldo)
// v8.5: PIN individual por empleado — env REWARDS_STAFF_PINS con pares
// "1111:Suheidi,2222:Eduardo" (acepta pin:nombre o nombre:pin). Con esto el
// Ledger registra QUIÉN hizo cada scan/canje/aplicación con su propio PIN.
const REWARDS_STAFF_PINS = (() => {
  const map = new Map();
  String(process.env.REWARDS_STAFF_PINS || '').split(',').forEach(pair => {
    const p = pair.split(':').map(s => s.trim()).filter(Boolean);
    if (p.length !== 2) return;
    if (/^\d{3,10}$/.test(p[0])) map.set(p[0], p[1]);
    else if (/^\d{3,10}$/.test(p[1])) map.set(p[1], p[0]);
  });
  return map;
})();
// (La antigua rewardsStaffFromPin() devolvía '' cuando no había PINs en el env
//  = acceso libre al mostrador. Se ELIMINÓ en la auditoría del 25-jul-2026:
//  la validación del PIN vive ahora dentro de rewardsStaffFrom(), fail-closed.)

// ── v8.6: LOGIN DE STAFF CON GOOGLE WORKSPACE ────────────────
// Decisión de Daniel 25-jul-2026: el mostrador se identifica con la cuenta
// corporativa (@filmorent.com), no con un PIN compartido. Al dar de baja a
// alguien en Workspace pierde el acceso solo; el Ledger registra su email real.
// El navegador manda el ID token de Google Identity Services; aquí se VERIFICA
// contra Google (endpoint oficial tokeninfo — evita meter dependencias nuevas
// al package.json de producción) y se exige: firma válida, aud == nuestro
// client id, no expirado, email verificado y dominio corporativo.
const REWARDS_GOOGLE_CLIENT_ID = process.env.REWARDS_GOOGLE_CLIENT_ID || '';
// Ventana para que el propio cliente reclame una renta desde su portal (self-service).
// En mostrador no aplica: ahí lo registra un empleado autenticado.
const REWARDS_ATRIB_DIAS = parseInt(process.env.REWARDS_ATRIB_DIAS || '14', 10);
const REWARDS_STAFF_DOMAIN = (process.env.REWARDS_STAFF_DOMAIN || 'filmorent.com').toLowerCase();
// Allowlist opcional: si trae emails, SOLO esos entran (aunque sean del dominio).
const REWARDS_STAFF_EMAILS = new Set(
  String(process.env.REWARDS_STAFF_EMAILS || '')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
);
// Cache de tokens ya verificados (clave = token, valor = {name, exp}). Evita
// pegarle a Google en cada request del mostrador; caduca con el propio token.
const rewardsTokenCache = new Map();

async function rewardsStaffFromGoogle(idToken) {
  const tok = String(idToken || '').trim();
  if (!tok) return null;
  const now = Math.floor(Date.now() / 1000);
  const hit = rewardsTokenCache.get(tok);
  if (hit && hit.exp > now + 30) return hit.name;
  try {
    const r = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(tok));
    if (!r.ok) return null;
    const t = await r.json();
    // aud: el token DEBE haber sido emitido para nuestro client id
    if (!REWARDS_GOOGLE_CLIENT_ID || t.aud !== REWARDS_GOOGLE_CLIENT_ID) {
      console.error('[rewards] token de Google con aud incorrecto');
      return null;
    }
    if (String(t.iss || '').indexOf('accounts.google.com') === -1) return null;
    const exp = parseInt(t.exp, 10) || 0;
    if (!exp || exp <= now) return null;
    if (String(t.email_verified) !== 'true') return null;
    const email = String(t.email || '').toLowerCase();
    // 'hd' (hosted domain) SOLO lo emite Google para cuentas Workspace del
    // dominio; el sufijo del email no basta (auditoría 25-jul). Se exige hd
    // Y que el email termine en el mismo dominio.
    const domain = String(t.hd || '').toLowerCase();
    if (domain !== REWARDS_STAFF_DOMAIN || email.slice(-(REWARDS_STAFF_DOMAIN.length + 1)) !== ('@' + REWARDS_STAFF_DOMAIN)) {
      console.error('[rewards] login de staff rechazado (dominio ' + domain + ')');
      return null;
    }
    if (REWARDS_STAFF_EMAILS.size > 0 && !REWARDS_STAFF_EMAILS.has(email)) {
      console.error('[rewards] login de staff fuera de la lista: ' + email);
      return null;
    }
    const name = String(t.name || '').trim() || email;
    if (rewardsTokenCache.size > 200) rewardsTokenCache.clear();
    rewardsTokenCache.set(tok, { name: name, exp: exp });
    return name;
  } catch (e) {
    console.error('[rewards] error verificando token de Google: ' + e.message);
    return null;
  }
}

// ── FAIL-CLOSED (auditoría 25-jul-2026) ──────────────────────
// Antes, "sin PINs configurados" significaba ACCESO LIBRE: al retirar el PIN
// para dejar solo Google, los 4 endpoints de mostrador (incluido /pagar, que
// mueve dinero real en Booqable) quedaban abiertos a internet. Ahora:
// sin credencial válida NO se pasa, y si NO hay ninguna forma de auth
// configurada el mostrador responde 503 (deshabilitado), nunca "pásale".
const REWARDS_STAFF_PROTECTED = !!(REWARDS_GOOGLE_CLIENT_ID || REWARDS_STAFF_PIN || REWARDS_STAFF_PINS.size > 0);
// Escotilla explícita SOLO para desarrollo local: se ignora en Render aunque
// alguien la ponga por error en el env de producción.
const REWARDS_STAFF_OPEN = process.env.REWARDS_STAFF_OPEN === '1' &&
  !process.env.RENDER && process.env.NODE_ENV !== 'production';
// Config rota (env de PINs escrita pero ninguna pareja válida) => tratar como
// mal configurado y bloquear, no como "sin protección".
const REWARDS_STAFF_PINS_BROKEN = !!String(process.env.REWARDS_STAFF_PINS || '').trim() && REWARDS_STAFF_PINS.size === 0;

// ── ANTI-FUERZA-BRUTA DEL PIN ────────────────────────────────
// La IP del cliente NO es confiable (cualquier header se puede falsificar y el
// portal pega directo al origin de Render), así que el candado REAL no puede
// depender de ella. Diseño (2ª auditoría 25-jul-2026):
//   · Solo cuentan los intentos que TRAEN un PIN — un anónimo no puede
//     provocar el bloqueo del equipo (evita el DoS).
//   · Tope GLOBAL de intentos de PIN por ventana, con backoff exponencial:
//     cada vez que se agota, el PIN queda deshabilitado el doble de tiempo
//     (15 min → 30 → 60 → … hasta 8 h). El login de Google NUNCA se bloquea,
//     así que el mostrador puede seguir operando durante un ataque.
//   · El contador por IP se conserva solo como señal para los logs.
const rewardsAuthFails = new Map();
const AUTH_WINDOW_MS = 15 * 60 * 1000;
const PIN_FAILS_MAX = 25;                 // intentos de PIN por ventana
let rewardsPinGate = { start: 0, n: 0, strikes: 0, blockedUntil: 0 };

function rewardsPinDisabled() {
  return Date.now() < rewardsPinGate.blockedUntil;
}
// Registra un intento FALLIDO que sí traía credencial de PIN.
function rewardsPinFail(ip) {
  const now = Date.now();
  if (now - rewardsPinGate.start > AUTH_WINDOW_MS) {
    rewardsPinGate.start = now;
    rewardsPinGate.n = 0;
    if (now - rewardsPinGate.blockedUntil > 6 * 3600 * 1000) rewardsPinGate.strikes = 0; // se calma solo
  }
  rewardsPinGate.n++;
  let b = rewardsAuthFails.get(ip);
  if (!b || now - b.start > AUTH_WINDOW_MS) { b = { start: now, n: 0 }; rewardsAuthFails.set(ip, b); }
  b.n++;
  if (rewardsAuthFails.size > 5000) rewardsAuthFails.clear();
  if (rewardsPinGate.n >= PIN_FAILS_MAX) {
    rewardsPinGate.strikes = Math.min(rewardsPinGate.strikes + 1, 5);
    const castigo = AUTH_WINDOW_MS * Math.pow(2, rewardsPinGate.strikes - 1); // 15m,30m,1h,2h,4h
    rewardsPinGate.blockedUntil = now + castigo;
    rewardsPinGate.n = 0;
    console.error('[rewards] PIN de staff DESHABILITADO ' + Math.round(castigo / 60000) +
      ' min por exceso de intentos fallidos (ultimo desde ' + ip + '). El login de Google sigue activo.');
  } else {
    console.error('[rewards] intento de PIN fallido desde ' + ip +
      ' (' + rewardsPinGate.n + '/' + PIN_FAILS_MAX + ' en la ventana global)');
  }
}

// Máximo 3 canjes por miembro cada 24 h (el portal del cliente no tiene login).
const rewardsRedeemsPorMiembro = new Map();
function rewardsRedeemAllowed(customerId) {
  const now = Date.now();
  let b = rewardsRedeemsPorMiembro.get(customerId);
  if (!b || now - b.start > 24 * 3600 * 1000) { b = { start: now, n: 0 }; rewardsRedeemsPorMiembro.set(customerId, b); }
  if (b.n >= 3) return false;
  b.n++;
  if (rewardsRedeemsPorMiembro.size > 5000) rewardsRedeemsPorMiembro.clear();
  return true;
}

// Comparación de secretos en tiempo constante (evita filtrar el PIN por timing)
function rewardsSecretEq(a, b) {
  const x = String(a || ''), y = String(b || '');
  let d = x.length ^ y.length;
  const n = Math.max(x.length, y.length);
  for (let i = 0; i < n; i++) d |= (x.charCodeAt(i) || 0) ^ (y.charCodeAt(i) || 0);
  return d === 0;
}

// Credenciales de una petición: header Authorization (preferido — no queda en
// logs ni en el referer), luego body, luego query (solo para el PIN).
function rewardsAuthSrc(req) {
  const h = String(req.headers.authorization || '');
  const bearer = /^Bearer\s+(.+)$/i.exec(h);
  const b = req.body || {};
  const q = req.query || {};
  return {
    id_token: (bearer && bearer[1]) || b.id_token || q.id_token || '',
    pin: b.pin || q.pin || ''
  };
}

// Identifica al staff de una petición: primero Google Workspace, si no hay
// token cae al PIN (respaldo). Devuelve el nombre a registrar en el Ledger,
// o null si no hay credencial válida.
async function rewardsStaffFrom(req) {
  const s = rewardsAuthSrc(req);
  const ip = rewardsClientIp(req);
  const tokenName = await rewardsStaffFromGoogle(s.id_token);
  if (tokenName) return tokenName;
  const pin = String(s.pin || '').trim();
  if (pin && !rewardsPinDisabled() && !REWARDS_STAFF_PINS_BROKEN) {
    // comparación de tiempo constante (no filtra el PIN por timing)
    if (REWARDS_STAFF_PINS.size > 0) {
      for (const [k, nombre] of REWARDS_STAFF_PINS) if (rewardsSecretEq(pin, k)) return nombre;
    }
    if (REWARDS_STAFF_PIN && rewardsSecretEq(pin, REWARDS_STAFF_PIN)) return 'staff';
  }
  // desarrollo local sin ninguna auth configurada
  if (!REWARDS_STAFF_PROTECTED && REWARDS_STAFF_OPEN) return 'dev';
  // SOLO cuentan los fallos que traían credencial: un anónimo no puede
  // provocar el bloqueo del PIN de todo el equipo
  if (pin) rewardsPinFail(ip);
  return null;
}

// Guard de los endpoints de mostrador. true = ya respondió (denegado).
function rewardsStaffDenied(res, staffName) {
  if (staffName) return false;
  if (!REWARDS_STAFF_PROTECTED) {
    res.status(503).json({ ok: false, error: 'mostrador deshabilitado: falta configurar el acceso de staff' });
    return true;
  }
  res.status(401).json({ ok: false, error: 'inicia sesion con tu cuenta @' + REWARDS_STAFF_DOMAIN + ' (o usa tu PIN)' });
  return true;
}

// LEGACY — Catalogo v1 (solo % de descuento). Ya NO lo usa nadie: desde la
// decision F0 (crédito calibrado, 23-jul-2026) el catálogo se personaliza por
// miembro con rewardsCatalogFor(). Se conserva solo como documentación/fallback.
const REWARDS_CATALOG = [
  { id: 1, name: '5% descuento en tu próxima renta', points: 100, value: 5 },
  { id: 2, name: '10% descuento en tu próxima renta', points: 250, value: 10 },
  { id: 3, name: '15% descuento en tu próxima renta', points: 500, value: 15 },
  { id: 4, name: '20% descuento en tu próxima renta', points: 800, value: 20 },
  { id: 5, name: '25% descuento en tu próxima renta', points: 1200, value: 25 }
];

// ── Crédito calibrado (decisión F0 de Daniel, 23-jul-2026) ──
// El premio deja de ser % y pasa a ser CRÉDITO EN PESOS con doble tope:
//   crédito del nivel = MIN(techo del nivel, 50% del ticket promedio del
//   miembro redondeado a múltiplos de $50), con piso de $100.
// Ids estables por nivel (el Ledger los referencia): 50→6 (escalón de entrada,
// nuevo), 100→1, 250→2, 500→3, 800→4, 1200→5. Reglas de mostrador que NO
// cambian (solo copy): 1 canje por orden, el folio no da cambio, la orden debe
// ser ≥ 2× el crédito.
const REWARDS_CREDIT_LEVELS = [
  { id: 6, points: 50,   cap_cents: 10000 },   // $100
  { id: 1, points: 100,  cap_cents: 25000 },   // $250
  { id: 2, points: 250,  cap_cents: 70000 },   // $700
  { id: 3, points: 500,  cap_cents: 160000 },  // $1,600
  { id: 4, points: 800,  cap_cents: 280000 },  // $2,800
  { id: 5, points: 1200, cap_cents: 450000 }   // $4,500
];

// "$1,600" estilo es-MX sin centavos (manual: no depende de ICU en el runtime).
function rewardsFormatMXN(cents) {
  const pesos = Math.round((cents || 0) / 100);
  return '$' + String(pesos).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Catálogo personalizado del miembro. DEDUPE: si el tope del ticket hace que
// dos niveles den el mismo crédito, solo se ofrece el de MENOS puntos — cada
// nivel listado debe dar un crédito estrictamente mayor que el anterior.
function rewardsCatalogFor(avgTicketCents) {
  // 50% del ticket promedio, redondeado a múltiplos de $50 (5000 centavos)
  const halfTicket = Math.round((avgTicketCents || 0) / 2 / 5000) * 5000;
  const catalog = [];
  let prevCredit = 0;
  for (const lvl of REWARDS_CREDIT_LEVELS) {
    const credit = Math.max(10000, Math.min(lvl.cap_cents, halfTicket)); // piso $100
    if (credit <= prevCredit) continue;
    prevCredit = credit;
    catalog.push({
      id: lvl.id,
      name: 'Crédito de ' + rewardsFormatMXN(credit) + ' en tu próxima renta',
      points: lvl.points,
      credito_cents: credit
    });
  }
  return catalog;
}

// Nivel por RENTAS DE LOS ÚLTIMOS 12 MESES (decisión de Daniel 24-jul-2026):
// el estatus se gana y se defiende cada año; los puntos canjeables NO caducan.
// min en centavos SIN IVA de revenue rodante de 12 meses (base ya sin
// exclusiones). Censo 24-jul: Plata $20k+/año = 57 clientes (top 5% de 1,114
// activos), Oro $100k+/año = 5 clientes.
const REWARDS_TIERS = [
  { name: 'Bronce', min_12m_cents: 0, discount: 0 },
  { name: 'Plata', min_12m_cents: 2000000, discount: 5 },   // $20,000 MXN/año
  { name: 'Oro', min_12m_cents: 10000000, discount: 10 }    // $100,000 MXN/año
];

// Cuentas que NO participan en el programa (decisión de Daniel 24-jul-2026):
// socios/internos. No aparecen en /member, /redeem, /pagar ni en el índice QR.
const REWARDS_EXCLUDED_CUSTOMER_IDS = new Set([
  '4263eb3e-1448-47a6-974d-73494fe8783b'  // Pasumecha Producciones (socio)
]);
// Órdenes que no acumulan puntos: ventas de equipo capturadas como orden.
// El programa premia renta recurrente; una compra de equipo es transacción
// única (reversible: quitar el número de esta lista si Daniel decide que
// las ventas sí acumulen). Ventas futuras: capturar la línea empezando con
// "VENTA" y queda excluida sola (regla en rewardsLineExcluded).
const REWARDS_EXCLUDED_ORDER_NUMBERS = new Set([
  10261  // Blackbear 8-jul-2026: venta Ronin 2 + Ready Rig GS ProArm ($130k)
]);

// CORS solo para /rewards/* (el portal vive en otro dominio).
const REWARDS_ORIGINS = [
  'https://rewards.filmorent.com',
  'https://rewards-filmorent.daniel-85f.workers.dev',
  'https://filmorent.com',
  'https://www.filmorent.com'
];
// Rate limit simple por IP (el endpoint es publico y devuelve datos de miembro):
// 30 requests por ventana de 5 min. En memoria — suficiente para un solo dyno.
const rewardsRate = new Map();
// IP aproximada del cliente, para el rate limit y el audit trail del Ledger.
// ⚠️ NO ES CONFIABLE para seguridad: el portal pega directo al origin de Render
// y cualquiera puede mandar los headers que quiera (2ª auditoría 25-jul-2026).
// Por eso el candado del PIN es un tope GLOBAL con backoff, no un bloqueo por
// IP. Aquí se prefiere el primer x-forwarded-for porque es el que trae la IP
// pública real del cliente en el tráfico legítimo (verificado en el Ledger).
function rewardsClientIp(req) {
  const xff = String(req.headers['x-forwarded-for'] || '').split(',').map(s => s.trim()).filter(Boolean);
  if (xff.length) return xff[0].slice(0, 60);
  const cf = String(req.headers['cf-connecting-ip'] || '').trim();
  if (cf) return cf.slice(0, 60);
  return String(req.ip || 'desconocida').slice(0, 60);
}
function rewardsClientUa(req) {
  return String(req.headers['user-agent'] || '').slice(0, 150);
}
function rewardsRateOk(ip) {
  const now = Date.now();
  let b = rewardsRate.get(ip);
  if (!b || now - b.start > 5 * 60 * 1000) { b = { start: now, n: 0 }; rewardsRate.set(ip, b); }
  b.n++;
  if (rewardsRate.size > 5000) rewardsRate.clear(); // tope de memoria
  return b.n <= 30;
}

app.use('/rewards', (req, res, next) => {
  const origin = req.headers.origin || '';
  if (REWARDS_ORIGINS.includes(origin) || /^http:\/\/localhost(:\d+)?$/.test(origin) || origin === 'null') {
    res.setHeader('Access-Control-Allow-Origin', origin);
  }
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  // 'Authorization' habilita mandar el ID token en header (fuera de la URL)
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (!BOOQABLE_API_KEY) return res.status(503).json({ ok: false, error: 'rewards deshabilitado: falta BOOQABLE_API_KEY en el env' });
  if (!rewardsRateOk(rewardsClientIp(req))) return res.status(429).json({ ok: false, error: 'demasiadas solicitudes, espera unos minutos' });
  next();
});

// GET a Booqable con retry simple en 429.
async function booqableGet(pathWithQuery, attempt) {
  const r = await fetch(BOOQABLE_BASE + pathWithQuery, {
    headers: { 'Authorization': 'Bearer ' + BOOQABLE_API_KEY, 'Accept': 'application/json' }
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return booqableGet(pathWithQuery, (attempt || 0) + 1);
  }
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    const err = new Error('Booqable ' + r.status + ': ' + body.slice(0, 200));
    err.status = r.status;
    throw err;
  }
  return r.json();
}

// Escritura a Booqable (POST/PATCH/DELETE) con el mismo retry en 429.
// Verificado 24-jul-2026 en orden de prueba: POST /lines con owner_id/owner_type
// crea la línea y los totales de la orden se recalculan solos.
async function booqableWrite(method, path, payload, attempt) {
  const r = await fetch(BOOQABLE_BASE + path, {
    method: method,
    headers: {
      'Authorization': 'Bearer ' + BOOQABLE_API_KEY,
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: payload === undefined ? undefined : JSON.stringify(payload)
  });
  if (r.status === 429 && (attempt || 0) < 2) {
    const ra = parseInt(r.headers.get('retry-after') || '2', 10);
    await new Promise(z => setTimeout(z, (ra || 2) * 1000));
    return booqableWrite(method, path, payload, (attempt || 0) + 1);
  }
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    const err = new Error('Booqable ' + r.status + ': ' + body.slice(0, 200));
    err.status = r.status;
    throw err;
  }
  return r.status === 204 ? null : r.json();
}

// QR de miembro: FLM-XX-YYYY-XXNX, deterministico del customer_id (FNV-1a 32-bit).
function rewardsQrCode(customerId) {
  let h = 0x811c9dc5;
  for (let i = 0; i < customerId.length; i++) {
    h ^= customerId.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  const L = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // sin I/O (se confunden con 1/0)
  const a = L[h % 24];
  const b = L[Math.floor(h / 24) % 24];
  const year = 2025 + (Math.floor(h / 576) % 2);
  const c = L[Math.floor(h / 1152) % 24];
  const n = Math.floor(h / 27648) % 10;
  const d = L[Math.floor(h / 276480) % 24];
  const e = Math.floor(h / 6635520) % 9;
  return 'FLM-' + a + b + '-' + year + '-' + c + n + d + e;
}

function rewardsTierFor(revenue12mCents) {
  for (let i = REWARDS_TIERS.length - 1; i >= 0; i--) {
    if (revenue12mCents >= REWARDS_TIERS[i].min_12m_cents) return REWARDS_TIERS[i];
  }
  return REWARDS_TIERS[0];
}

// Busca el customer por email exacto en Booqable. null si no existe.
async function rewardsFindCustomer(email) {
  const d = await booqableGet('/customers?filter[email]=' + encodeURIComponent(email) + '&page[size]=1');
  const c = (d.data || [])[0];
  return c || null;
}

// Lineas que NO generan puntos (regla de Daniel 22-jul-2026): subrenta ELSEPC
// (90% no es ingreso nuestro) + cargos de servicio que no son renta de equipo
// (pintura del estudio, personal: staff/encargados/operadores/gaffer). El equipo
// del bono del estudio NO aparece como linea cobrada (va dentro del precio del
// paquete), asi que no requiere exclusion. Nombres verificados contra el catalogo.
const REWARDS_EXCLUDE_SUBSTR = [
  'elsepc',                    // ELSEPC PureBB trifasico (subrenta)
  'pintura y regreso',         // Pintura y regreso a estado original
  'hora extra personal',
  'operador prompter',
  'encargado de estudio',      // todas las variantes (Alfredo/Barush/... y "ya no usar")
  'gaffer'                     // por si se captura personal como linea libre
];
function rewardsLineExcluded(title) {
  const t = (title || '').toLowerCase().trim();
  if (!t) return false;
  if (REWARDS_EXCLUDE_SUBSTR.some(k => t.indexOf(k) !== -1)) return true;
  // producto "Staff" (personal) — match estricto para no rozar nombres de equipo
  if (t === 'staff' || t.indexOf('staff ') === 0 || t.indexOf('staff -') === 0) return true;
  // ventas de equipo capturadas como línea libre ("VENTA - Ronin 2...") —
  // prefijo estricto para no rozar títulos que contengan "ventana" etc.
  if (t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0) return true;
  return false;
}

// Calcula puntos ganados + historial de un customer, excluyendo las lineas de
// REWARDS_EXCLUDE_SUBSTR. Verificado: sum(grand_total sin draft) === revenue_in_cents.
async function rewardsComputeEarned(customerId) {
  // 1) todas las ordenes del cliente
  const orders = [];
  for (let page = 1; page <= 10; page++) {
    const od = await booqableGet('/orders?filter[customer_id]=' + customerId +
      '&sort=-created_at&page[size]=100&page[number]=' + page);
    const data = od.data || [];
    orders.push(...data);
    if (data.length < 100) break;
  }
  const countable = orders.filter(o => {
    const a = o.attributes || {};
    if (a.status === 'draft' || a.status === 'concept' || a.status === 'canceled') return false;
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(a.number)) return false; // ventas de equipo
    return true;
  });

  // 2) lineas ELSEPC de esas ordenes, en lotes de 25 order_ids
  const elsepcByOrder = {};
  const ids = countable.map(o => o.id);
  for (let i = 0; i < ids.length; i += 25) {
    const batch = ids.slice(i, i + 25).join(',');
    for (let lpage = 1; lpage <= 10; lpage++) {
      const ld = await booqableGet('/lines?filter[order_id]=' + batch +
        '&page[size]=100&page[number]=' + lpage);
      const data = ld.data || [];
      for (const l of data) {
        const la = l.attributes || {};
        if (la.archived) continue;
        if (!rewardsLineExcluded(la.title)) continue;
        elsepcByOrder[la.order_id] = (elsepcByOrder[la.order_id] || 0) + (la.price_in_cents || 0);
      }
      if (data.length < 100) break;
    }
  }

  // 3) base de puntos por orden = grand_total - ELSEPC (convertido a base sin IVA)
  let totalBaseCents = 0;
  let totalElsepcCents = 0;
  // revenue rodante de 12 meses (base del NIVEL): fecha de la renta
  // (starts_at) o de creación si no la hay
  const cutoff12m = new Date(Date.now() - 365 * 24 * 3600 * 1000).toISOString();
  let revenue12mCents = 0;
  const orderRows = countable.map(o => {
    const a = o.attributes || {};
    const g = a.grand_total_in_cents || 0;
    const gt = a.grand_total_with_tax_in_cents || 0;
    const elWithTax = elsepcByOrder[o.id] || 0;
    const ratio = gt ? (g / gt) : 1;
    const elBase = Math.min(Math.round(elWithTax * ratio), g);
    totalBaseCents += g;
    totalElsepcCents += elBase;
    if (String(a.starts_at || a.created_at || '') >= cutoff12m) {
      revenue12mCents += Math.max(0, g - elBase);
    }
    return {
      id: o.id,
      number: a.number,
      status: a.status,
      total_cents: g,
      total_with_tax_cents: gt,
      elsepc_excluded_cents: elBase,
      points: Math.floor((g - elBase) / 100 / 100),
      item_count: a.item_count || 0,
      starts_at: a.starts_at,
      stops_at: a.stops_at,
      created_at: a.created_at
    };
  });

  const pointsBaseCents = totalBaseCents - totalElsepcCents;
  return {
    orders: orderRows,
    revenue_cents: totalBaseCents,
    revenue_12m_cents: revenue12mCents,
    elsepc_excluded_cents: totalElsepcCents,
    points_earned: Math.floor(pointsBaseCents / 100 / 100)
  };
}

// Lee del Ledger (Apps Script doGet) los canjes de un customer.
// Devuelve null si el Ledger no esta configurado o fallo (degradar con flag).
async function rewardsLedgerSummary(customerId) {
  if (!REWARDS_SHEETS_URL) return null;
  try {
    const r = await fetch(REWARDS_SHEETS_URL + '?action=member&customer_id=' + encodeURIComponent(customerId), { redirect: 'follow' });
    if (!r.ok) return null;
    const j = await r.json().catch(() => null);
    if (!j || j.ok === false) return null;
    return {
      redeemed_points: j.redeemed_points || 0,
      redemptions: j.redemptions || [],
      // 28-jul: órdenes de otros que este miembro pidió (suman) y órdenes propias
      // cuyos puntos cedió a quien las pidió (restan)
      atribuciones_ganadas: j.atribuciones_ganadas || [],
      atribuciones_cedidas: j.atribuciones_cedidas || []
    };
  } catch (e) {
    console.error('rewards ledger read error: ' + e.message);
    return null;
  }
}

// Todo texto que acaba en una celda del Sheet: recortado y neutralizado contra
// inyección de fórmulas (un nombre de cliente que empiece con = + - @ lo
// interpretaría Sheets como fórmula). Auditoría 25-jul-2026.
function rewardsCellSafe(v) {
  if (typeof v !== 'string') return v;
  const s = v.slice(0, 300);
  return /^[=+\-@\t\r]/.test(s) ? "'" + s : s;
}

// Escribe una fila al Ledger (Apps Script doPost). true/false.
async function rewardsLedgerWrite(row) {
  if (!REWARDS_SHEETS_URL) return false;
  const safe = {};
  Object.keys(row || {}).forEach(k => { safe[k] = rewardsCellSafe(row[k]); });
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(safe),
      redirect: 'follow'
    });
    if (!r.ok) { console.error('rewards ledger write failed: ' + r.status); return false; }
    const j = await r.json().catch(() => null);
    return !j || j.ok !== false;
  } catch (e) {
    console.error('rewards ledger write error: ' + e.message);
    return false;
  }
}

function rewardsCleanName(name) {
  return (name || '').split('/')[0].trim();
}

// Arma la respuesta completa de miembro (usada por /member y /scan).
async function rewardsBuildMember(customer) {
  const a = customer.attributes || {};
  const earned = await rewardsComputeEarned(customer.id);
  const ledger = await rewardsLedgerSummary(customer.id);
  const redeemed = ledger ? ledger.redeemed_points : 0;
  // Atribuciones: en renta audiovisual quien ELIGE el proveedor (DP/freelance) no
  // siempre es quien PAGA (la productora). Si pidió que los puntos fueran suyos,
  // se los sumamos a él y se los restamos al titular de la orden. Una orden solo
  // puede estar atribuida a una persona (el Ledger lo garantiza).
  const ganadas = (ledger && ledger.atribuciones_ganadas) || [];
  const cedidas = (ledger && ledger.atribuciones_cedidas) || [];
  const puntosGanados = ganadas.reduce((s, x) => s + (Number(x.puntos) || 0), 0);
  const puntosCedidos = cedidas.reduce((s, x) => s + (Number(x.puntos) || 0), 0);
  const earnedTotal = Math.max(0, earned.points_earned + puntosGanados - puntosCedidos);
  const available = Math.max(0, earnedTotal - redeemed);
  const tier = rewardsTierFor(earned.revenue_12m_cents);
  // Ticket promedio para el crédito calibrado: revenue SIN exclusiones ELSEPC
  // (el mismo que ya calcula rewardsComputeEarned) / # de órdenes contables.
  const countableOrders = earned.orders.length;
  const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
  return {
    member: {
      customer_id: customer.id,
      name: rewardsCleanName(a.name),
      full_name: a.name || '',
      email: a.email || '',
      member_id: 'FLM-' + String(a.number || '0').padStart(5, '0'),
      qr_code: rewardsQrCode(customer.id),
      member_since: a.created_at,
      last_order_at: a.last_order_at || a.latest_order_at || null,
      order_count: a.order_count || 0,
      avg_order_cents: a.average_order_value_in_cents || 0,
      avg_ticket_cents: avgTicketCents
    },
    points: {
      earned: earnedTotal,                    // ya incluye atribuciones
      earned_propias: earned.points_earned,   // solo sus órdenes en Booqable
      ganados_por_atribucion: puntosGanados,  // rentas que pidió y pagó otro
      cedidos_por_atribucion: puntosCedidos,  // rentas suyas que pidió otro
      redeemed: redeemed,
      available: available,
      revenue_cents: earned.revenue_cents,
      revenue_12m_cents: earned.revenue_12m_cents,
      elsepc_excluded_cents: earned.elsepc_excluded_cents
    },
    atribuciones: { ganadas: ganadas, cedidas: cedidas },
    tier: {
      name: tier.name,
      discount: tier.discount,
      base_12m_cents: earned.revenue_12m_cents
    },
    orders: earned.orders,
    redemptions: ledger ? ledger.redemptions : [],
    catalog: rewardsCatalogFor(avgTicketCents),
    ledger_ok: !!ledger
  };
}

// ── GET /rewards/member?email= ──────────────────────────────
app.get('/rewards/member', async (req, res) => {
  const email = String(req.query.email || '').trim().toLowerCase();
  if (!email || email.indexOf('@') === -1) {
    return res.status(400).json({ ok: false, error: 'email invalido' });
  }
  try {
    const customer = await rewardsFindCustomer(email);
    if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const out = await rewardsBuildMember(customer);
    console.log('[rewards] member ' + email + ' -> ' + out.points.available + ' pts disponibles (' +
      out.points.earned + ' ganados, ' + out.points.redeemed + ' canjeados, ledger=' + out.ledger_ok + ')');
    return res.json(Object.assign({ ok: true }, out));
  } catch (e) {
    console.error('[rewards] member error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando Booqable, intenta de nuevo' });
  }
});

// ── POST /rewards/redeem  {email, reward_id} ────────────────
app.post('/rewards/redeem', async (req, res) => {
  const email = String((req.body || {}).email || '').trim().toLowerCase();
  const rewardId = parseInt((req.body || {}).reward_id, 10);
  if (!email || email.indexOf('@') === -1) return res.status(400).json({ ok: false, error: 'email invalido' });
  if (!Number.isFinite(rewardId)) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'canjes temporalmente deshabilitados (Ledger no configurado)' });

  try {
    const customer = await rewardsFindCustomer(email);
    if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customer.id)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }

    // Recalcular saldo Y catálogo personalizado EN VIVO (no confiar en el
    // cliente): el reward_id se valida contra el catálogo calibrado del propio
    // miembro, no contra una tabla estática.
    const earned = await rewardsComputeEarned(customer.id);
    const ledger = await rewardsLedgerSummary(customer.id);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const countableOrders = earned.orders.length;
    const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
    const catalog = rewardsCatalogFor(avgTicketCents);
    const reward = catalog.find(r => r.id === rewardId);
    if (!reward) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
    // saldo REAL: propias + ganadas por atribución − cedidas − canjeadas
    const available = Math.max(0, earned.points_earned
      + ((ledger.atribuciones_ganadas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ((ledger.atribuciones_cedidas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ledger.redeemed_points);
    if (available < reward.points) {
      return res.status(409).json({ ok: false, error: 'puntos insuficientes', points_available: available });
    }

    // Tope de canjes por miembro/día: el portal del cliente no tiene login
    // (entra con su email), así que sin esto un tercero podría quemarle los
    // puntos a un miembro creando canjes en cadena (auditoría 25-jul-2026).
    if (!rewardsRedeemAllowed(customer.id)) {
      return res.status(429).json({ ok: false, error: 'ya hiciste varios canjes hoy; escríbenos por WhatsApp para ayudarte' });
    }

    const credito = rewardsFormatMXN(reward.credito_cents);
    const folio = 'RWD-' + Date.now().toString(36).toUpperCase() + '-' +
      Math.random().toString(36).slice(2, 6).toUpperCase();
    const wrote = await rewardsLedgerWrite({
      tipo: 'canje',
      folio: folio,
      fecha: new Date().toISOString(),
      customer_id: customer.id,
      email: email,
      nombre: rewardsCleanName((customer.attributes || {}).name),
      reward_id: reward.id,
      reward_name: reward.name,               // ej. "Crédito de $700 en tu próxima renta"
      puntos: reward.points,
      descuento_pct: 0,                       // ya no hay %: el premio es crédito
      credito_mxn: reward.credito_cents / 100, // número en pesos (el .gs lo ignora sin romper)
      estado: 'pendiente',
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });
    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo registrar el canje, intenta de nuevo' });

    console.log('[rewards] canje ' + folio + ' ' + email + ' -' + reward.points + ' pts (crédito ' + credito + ')');
    return res.json({
      ok: true,
      folio: folio,
      reward: reward,
      points_available: available - reward.points,
      // el Ledger esta configurado (503 arriba si no) y el .gs manda el correo
      // de confirmacion al registrar la fila del canje
      email_confirmacion: true,
      instrucciones: 'Presenta el folio ' + folio + ' al confirmar tu próxima renta para aplicar tu crédito de ' +
        credito + '. Aplica en rentas de al menos el doble del crédito; el folio no da cambio.'
    });
  } catch (e) {
    console.error('[rewards] redeem error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error procesando el canje, intenta de nuevo' });
  }
});

// ── POST /rewards/scan  {code, pin?, order_number?, staff_name?} ──
// Resuelve el QR de miembro contra un indice qr->customer construido paginando
// /customers (cache 12h; se reconstruye si el codigo no aparece).
let rewardsQrIndex = null;       // Map code -> {id, name, email, number}
let rewardsQrIndexAt = 0;
let rewardsQrAmbiguous = {};     // codigos con colision (no resolubles)

async function rewardsBuildQrIndex() {
  const idx = new Map();
  const ambiguous = {};
  const seen = new Set(); // dedupe por id, por si acaso
  for (let page = 1; page <= 60; page++) {
    // sort=number (unico y estable): con sort=created_at los empates hacen que la
    // paginacion REPITA ~500 clientes y OMITA otros ~500 (verificado 22-jul-2026)
    const d = await booqableGet('/customers?page[size]=100&page[number]=' + page + '&sort=number');
    const data = d.data || [];
    for (const c of data) {
      if (seen.has(c.id)) continue;
      seen.add(c.id);
      if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(c.id)) continue; // socios/internos fuera
      const a = c.attributes || {};
      const code = rewardsQrCode(c.id);
      if (idx.has(code)) {
        // colision REAL solo si son clientes distintos
        if (idx.get(code).id !== c.id) {
          ambiguous[code] = true;
          console.error('[rewards] COLISION de QR ' + code + ': ' + idx.get(code).id + ' vs ' + c.id);
        }
        continue;
      }
      idx.set(code, { id: c.id, name: rewardsCleanName(a.name), email: a.email || '', number: a.number });
    }
    if (data.length < 100) break;
  }
  rewardsQrIndex = idx;
  rewardsQrAmbiguous = ambiguous;
  rewardsQrIndexAt = Date.now();
  console.log('[rewards] indice QR: ' + idx.size + ' miembros, ' + Object.keys(ambiguous).length + ' colisiones');
}

app.post('/rewards/scan', async (req, res) => {
  const body = req.body || {};
  const code = String(body.code || '').trim().toUpperCase();
  const staffScan = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffScan)) return;
  if (!/^FLM-[A-Z]{2}-\d{4}-[A-Z]\d[A-Z]\d$/.test(code)) {
    return res.status(400).json({ ok: false, error: 'formato de codigo invalido (esperado FLM-XX-YYYY-XNXN)' });
  }
  try {
    const stale = !rewardsQrIndex || (Date.now() - rewardsQrIndexAt) > 12 * 3600 * 1000;
    if (stale) await rewardsBuildQrIndex();
    let hit = rewardsQrIndex.get(code);
    if (!hit && !stale) { await rewardsBuildQrIndex(); hit = rewardsQrIndex.get(code); }
    if (rewardsQrAmbiguous[code]) {
      return res.status(409).json({ ok: false, error: 'codigo ambiguo, identifica al miembro por email' });
    }
    if (!hit) return res.status(404).json({ ok: false, error: 'codigo no encontrado' });

    // resumen completo del miembro (puntos en vivo)
    const cd = await booqableGet('/customers/' + hit.id);
    const customer = cd.data;
    const out = await rewardsBuildMember(customer);

    const logged = await rewardsLedgerWrite({
      tipo: 'scan',
      fecha: new Date().toISOString(),
      code: code,
      customer_id: hit.id,
      nombre: out.member.name,
      email: out.member.email,
      order_number: body.order_number || '',
      // identidad VERIFICADA (Google/PIN); nunca el staff_name que mande el
      // navegador — ese campo ya no es identidad (auditoría 25-jul)
      staff_name: staffScan,
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });

    console.log('[rewards] scan ' + code + ' -> ' + out.member.name + ' (logged=' + logged + ')');
    return res.json(Object.assign({ ok: true, logged: logged }, out));
  } catch (e) {
    console.error('[rewards] scan error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error resolviendo el codigo, intenta de nuevo' });
  }
});

// ── POST /rewards/pagar  {code|email, reward_id, order_number, staff_name?, pin?} ──
// "Pagar con puntos" en una escaneada (aprobado por Daniel 24-jul-2026): el staff
// escanea el QR del miembro (o captura su email), elige el crédito de su catálogo
// calibrado y captura el # de orden; el server APLICA el descuento en Booqable
// (línea negativa CON IVA — el cliente ve el crédito exacto en pesos) y registra
// el canje ya 'aplicado' en el Ledger. El folio queda solo como registro interno.
// Candados: PIN de staff, saldo y catálogo recalculados en vivo, orden ≥ 2× el
// crédito, 1 crédito Rewards por orden, orden cancelada rechazada.
app.post('/rewards/pagar', async (req, res) => {
  const body = req.body || {};
  const staffPagar = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffPagar)) return;
  const code = String(body.code || '').trim().toUpperCase();
  const email = String(body.email || '').trim().toLowerCase();
  const rewardId = parseInt(body.reward_id, 10);
  const orderNumber = parseInt(body.order_number, 10);
  if (!Number.isFinite(rewardId)) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
  if (!Number.isFinite(orderNumber)) return res.status(400).json({ ok: false, error: 'order_number invalido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'pagos con puntos deshabilitados (Ledger no configurado)' });
  try {
    // 1) resolver al miembro por QR o por email
    let customerId = null;
    if (code) {
      const stale = !rewardsQrIndex || (Date.now() - rewardsQrIndexAt) > 12 * 3600 * 1000;
      if (stale) await rewardsBuildQrIndex();
      let hit = rewardsQrIndex.get(code);
      if (!hit && !stale) { await rewardsBuildQrIndex(); hit = rewardsQrIndex.get(code); }
      if (rewardsQrAmbiguous[code]) return res.status(409).json({ ok: false, error: 'codigo ambiguo, identifica al miembro por email' });
      if (!hit) return res.status(404).json({ ok: false, error: 'codigo no encontrado' });
      customerId = hit.id;
    } else if (email && email.indexOf('@') !== -1) {
      const c = await rewardsFindCustomer(email);
      if (!c) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
      customerId = c.id;
    } else {
      return res.status(400).json({ ok: false, error: 'manda code (QR) o email del miembro' });
    }
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(customerId)) {
      return res.status(403).json({ ok: false, error: 'esta cuenta no participa en Filmorent Rewards' });
    }
    const cd = await booqableGet('/customers/' + customerId);
    const customer = cd.data;

    // 2) saldo + catálogo calibrado EN VIVO (no confiar en el cliente)
    const earned = await rewardsComputeEarned(customerId);
    const ledger = await rewardsLedgerSummary(customerId);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const countableOrders = earned.orders.length;
    const avgTicketCents = countableOrders ? Math.round(earned.revenue_cents / countableOrders) : 0;
    const reward = rewardsCatalogFor(avgTicketCents).find(r => r.id === rewardId);
    if (!reward) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
    // saldo REAL: propias + ganadas por atribución − cedidas − canjeadas
    const available = Math.max(0, earned.points_earned
      + ((ledger.atribuciones_ganadas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ((ledger.atribuciones_cedidas || []).reduce((s, x) => s + (Number(x.puntos) || 0), 0))
      - ledger.redeemed_points);
    if (available < reward.points) {
      return res.status(409).json({ ok: false, error: 'puntos insuficientes', points_available: available });
    }

    // 3) la orden destino: existe, no cancelada, regla 2x, sin crédito previo
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(orderNumber)) {
      return res.status(409).json({ ok: false, error: 'los creditos Rewards aplican solo en rentas, no en ventas de equipo' });
    }
    const od = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const order = (od.data || [])[0];
    if (!order) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oa = order.attributes || {};
    if (oa.status === 'canceled') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' esta cancelada' });
    }
    const totalWithTax = oa.grand_total_with_tax_in_cents || 0;
    if (totalWithTax < reward.credito_cents * 2) {
      return res.status(409).json({
        ok: false,
        error: 'la orden debe ser de al menos ' + rewardsFormatMXN(reward.credito_cents * 2) +
          ' (2x el credito de ' + rewardsFormatMXN(reward.credito_cents) + '); total actual con IVA: ' + rewardsFormatMXN(totalWithTax)
      });
    }
    const ld = await booqableGet('/lines?filter[order_id]=' + order.id + '&page[size]=100');
    const lineasOrden = (ld.data || []).filter(l => !((l.attributes || {}).archived));
    const yaTiene = lineasOrden.some(l =>
      String((l.attributes || {}).title || '').toLowerCase().indexOf('filmorent rewards') === 0);
    if (yaTiene) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' ya tiene un credito Rewards aplicado' });
    }
    // regla de Daniel 24-jul-2026: los puntos NO aplican a ventas de equipo —
    // si la orden trae una línea de venta (prefijo "VENTA"), se rechaza
    const esVenta = lineasOrden.some(l => {
      const t = String((l.attributes || {}).title || '').toLowerCase().trim();
      return t.indexOf('venta ') === 0 || t.indexOf('venta-') === 0 || t.indexOf('venta:') === 0;
    });
    if (esVenta) {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' incluye venta de equipo: los creditos Rewards aplican solo en rentas' });
    }

    // 4) aplicar el descuento en Booqable
    const folio = 'RWD-' + Date.now().toString(36).toUpperCase() + '-' +
      Math.random().toString(36).slice(2, 6).toUpperCase();
    const credito = rewardsFormatMXN(reward.credito_cents);
    await booqableWrite('POST', '/lines', {
      data: {
        type: 'lines',
        attributes: {
          owner_id: order.id,
          owner_type: 'orders',
          title: 'Filmorent Rewards - credito ' + credito + ' (' + folio + ')',
          quantity: 1,
          price_each_in_cents: -reward.credito_cents
        }
      }
    });

    // 5) registrar en el Ledger: fila de canje (manda el email al cliente) y
    //    de inmediato marcarla 'aplicado' con la orden y el staff. Si el Ledger
    //    falla DESPUÉS de aplicar la línea, avisar para registro manual.
    const staffName = staffPagar;   // identidad verificada, no el body
    const wrote = await rewardsLedgerWrite({
      tipo: 'canje',
      folio: folio,
      fecha: new Date().toISOString(),
      customer_id: customerId,
      email: (customer.attributes || {}).email || '',
      nombre: rewardsCleanName((customer.attributes || {}).name),
      reward_id: reward.id,
      reward_name: reward.name,
      puntos: reward.points,
      descuento_pct: 0,
      credito_mxn: reward.credito_cents / 100,
      estado: 'pendiente',
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });
    let aplicadoEnLedger = false;
    if (wrote) {
      aplicadoEnLedger = await rewardsLedgerWrite({
        tipo: 'aplicar',
        folio: folio,
        order_number: String(orderNumber),
        staff_name: staffName || 'auto (pagar con puntos)'
      });
    }

    // total nuevo de la orden (recalculado por Booqable)
    let nuevoTotal = null;
    try {
      const od2 = await booqableGet('/orders/' + order.id);
      nuevoTotal = ((od2.data || {}).attributes || {}).grand_total_with_tax_in_cents;
    } catch (e2) { /* solo informativo */ }

    console.log('[rewards] pagar ' + folio + ' ' + (customer.attributes || {}).email + ' -' + reward.points +
      ' pts -> orden #' + orderNumber + ' (' + credito + ', ledger=' + (wrote ? (aplicadoEnLedger ? 'aplicado' : 'pendiente') : 'FALLO') + ')');
    return res.json({
      ok: true,
      folio: folio,
      credito_mxn: reward.credito_cents / 100,
      puntos_usados: reward.points,
      points_restantes: available - reward.points,
      order_number: orderNumber,
      nuevo_total_mxn: nuevoTotal === null ? null : nuevoTotal / 100,
      ledger_ok: !!wrote,
      advertencia: wrote ? null : 'el descuento SE APLICO en Booqable pero el Ledger no respondio: anota el folio ' + folio + ' manualmente en el Sheet'
    });
  } catch (e) {
    console.error('[rewards] pagar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el credito, intenta de nuevo' });
  }
});

// ── POST /rewards/atribuir ──────────────────────────────────
// "Los puntos de esta renta son de quien la pidió" (28-jul-2026).
// En renta audiovisual el DP/freelance ELIGE el proveedor pero la productora PAGA:
// si no le acreditamos los puntos a quien decide, el programa no influye la decisión.
// La orden en Booqable NO cambia de dueño (factura y cobranza intactas); solo se le
// pone una etiqueta `rw:<código>` para que el equipo lo vea en su propio sistema.
//
// Body: { order_number, code|email, canal?, pin?/id_token? } — o self-service:
//        { order_number, email_solicitante } con la sesión del propio cliente.
// Canales: 'mostrador' (staff autenticado), 'whatsapp' (AI/staff), 'cliente' (self-service).
app.post('/rewards/atribuir', async (req, res) => {
  const body = req.body || {};
  const selfService = String(body.canal || '') === 'cliente';
  let quienRegistra = 'cliente (self-service)';
  if (!selfService) {
    const staff = await rewardsStaffFrom(req);
    if (rewardsStaffDenied(res, staff)) return;
    quienRegistra = staff;
  }
  const orderNumber = parseInt(body.order_number, 10);
  if (!Number.isFinite(orderNumber)) return res.status(400).json({ ok: false, error: 'order_number invalido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'atribuciones deshabilitadas (Ledger no configurado)' });

  try {
    // 1) el beneficiario: por QR (mostrador) o por email (self-service / WhatsApp)
    let beneficiario = null;
    const code = String(body.code || '').trim().toUpperCase();
    const email = String(body.email || '').trim().toLowerCase();
    if (code) {
      const stale = !rewardsQrIndex || (Date.now() - rewardsQrIndexAt) > 12 * 3600 * 1000;
      if (stale) await rewardsBuildQrIndex();
      let hit = rewardsQrIndex.get(code);
      if (!hit && !stale) { await rewardsBuildQrIndex(); hit = rewardsQrIndex.get(code); }
      if (rewardsQrAmbiguous[code]) return res.status(409).json({ ok: false, error: 'codigo ambiguo, usa el email' });
      if (!hit) return res.status(404).json({ ok: false, error: 'codigo no encontrado' });
      const cd = await booqableGet('/customers/' + hit.id);
      beneficiario = cd.data;
    } else if (email && email.indexOf('@') !== -1) {
      beneficiario = await rewardsFindCustomer(email);
      if (!beneficiario) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });
    } else {
      return res.status(400).json({ ok: false, error: 'manda el code (QR) o el email de quien acumula' });
    }
    if (REWARDS_EXCLUDED_CUSTOMER_IDS.has(beneficiario.id)) {
      return res.status(403).json({ ok: false, error: 'esa cuenta no participa en Filmorent Rewards' });
    }

    // 2) la orden: debe existir, no estar cancelada ni ser venta de equipo
    if (REWARDS_EXCLUDED_ORDER_NUMBERS.has(orderNumber)) {
      return res.status(409).json({ ok: false, error: 'las ventas de equipo no acumulan puntos' });
    }
    const od = await booqableGet('/orders?filter[number]=' + orderNumber + '&page[size]=2');
    const order = (od.data || [])[0];
    if (!order) return res.status(404).json({ ok: false, error: 'no existe la orden #' + orderNumber });
    const oa = order.attributes || {};
    if (oa.status === 'canceled' || oa.status === 'draft' || oa.status === 'concept') {
      return res.status(409).json({ ok: false, error: 'la orden #' + orderNumber + ' no cuenta para puntos (' + oa.status + ')' });
    }
    if (String(oa.customer_id) === String(beneficiario.id)) {
      return res.status(409).json({ ok: false, error: 'esa orden ya es de esa cuenta: los puntos le llegan solos' });
    }
    // Self-service: solo rentas recientes (evita que alguien reclame el histórico ajeno)
    const dias = (Date.now() - new Date(oa.starts_at || oa.created_at).getTime()) / 86400000;
    if (selfService && dias > REWARDS_ATRIB_DIAS) {
      return res.status(409).json({ ok: false, error: 'esa renta tiene más de ' + REWARDS_ATRIB_DIAS + ' días; pídelo en el mostrador' });
    }

    // 3) puntos de esa orden (misma regla de siempre: base sin IVA, sin líneas excluidas)
    const gt = oa.grand_total_in_cents || 0;
    const gtt = oa.grand_total_with_tax_in_cents || 0;
    let excl = 0;
    try {
      const ld = await booqableGet('/lines?filter[order_id]=' + order.id + '&page[size]=100');
      const ratio = gtt ? (gt / gtt) : 1;
      for (const l of (ld.data || [])) {
        const la = l.attributes || {};
        if (la.archived || !rewardsLineExcluded(la.title)) continue;
        excl += Math.round((la.price_in_cents || 0) * ratio);
      }
    } catch (e2) { /* si falla, se atribuye el total */ }
    const baseCents = Math.max(0, gt - excl);
    const puntos = Math.floor(baseCents / 100 / 100);
    if (puntos <= 0) return res.status(409).json({ ok: false, error: 'esa orden no genera puntos' });

    // 4) titular actual (para el registro y para restarle los puntos)
    let titularNombre = '';
    try {
      const td = await booqableGet('/customers/' + oa.customer_id);
      titularNombre = rewardsCleanName((td.data.attributes || {}).name);
    } catch (e3) { /* informativo */ }

    // 5) registrar en el Ledger (rechaza si la orden ya tiene dueño de puntos)
    const wrote = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'atribucion',
        fecha: new Date().toISOString(),
        order_number: String(orderNumber),
        order_id: order.id,
        titular_id: oa.customer_id,
        titular_nombre: rewardsCellSafe(titularNombre),
        beneficiario_id: beneficiario.id,
        beneficiario_nombre: rewardsCellSafe(rewardsCleanName((beneficiario.attributes || {}).name)),
        beneficiario_email: (beneficiario.attributes || {}).email || '',
        monto_mxn: baseCents / 100,
        puntos: puntos,
        canal: body.canal || 'mostrador',
        registrado_por: rewardsCellSafe(quienRegistra),
        notas: rewardsCellSafe(String(body.notas || '')),
        ip: rewardsClientIp(req),
        ua: rewardsClientUa(req)
      }),
      redirect: 'follow'
    }).then(r => r.json()).catch(() => null);

    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo registrar, intenta de nuevo' });
    if (wrote.ok === false) {
      return res.status(409).json({
        ok: false,
        error: wrote.error === 'orden ya atribuida'
          ? 'los puntos de la orden #' + orderNumber + ' ya son de ' + (wrote.beneficiario || 'otra cuenta')
          : (wrote.error || 'no se pudo registrar'),
        beneficiario: wrote.beneficiario, registrado_por: wrote.registrado_por, fecha: wrote.fecha
      });
    }

    // 6) espejo visible en Booqable: etiqueta en la orden (no cambia titular ni factura)
    let etiquetada = false;
    try {
      const tags = Array.isArray(oa.tag_list) ? oa.tag_list.slice() : [];
      const etiqueta = 'rw:' + rewardsQrCode(beneficiario.id);
      if (tags.indexOf(etiqueta) === -1) tags.push(etiqueta);
      await booqableWrite('PATCH', '/orders/' + order.id, {
        data: { id: order.id, type: 'orders', attributes: { tag_list: tags } }
      });
      etiquetada = true;
    } catch (e4) {
      console.error('[rewards] no se pudo etiquetar la orden ' + orderNumber + ': ' + e4.message);
    }

    console.log('[rewards] atribucion orden ' + orderNumber + ' (' + puntos + ' pts) -> ' +
      rewardsCleanName((beneficiario.attributes || {}).name) + ' | canal ' + (body.canal || 'mostrador') +
      ' | por ' + quienRegistra + (etiquetada ? '' : ' | SIN etiqueta en Booqable'));

    return res.json({
      ok: true,
      order_number: orderNumber,
      puntos: puntos,
      beneficiario: rewardsCleanName((beneficiario.attributes || {}).name),
      beneficiario_email: (beneficiario.attributes || {}).email || '',
      titular: titularNombre,
      etiquetada_en_booqable: etiquetada,
      registrado_por: quienRegistra
    });
  } catch (e) {
    console.error('[rewards] atribuir error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error registrando la atribucion, intenta de nuevo' });
  }
});

// ── GET /rewards/atribucion?n=1234 ──────────────────────────
// ¿esta orden ya tiene dueño de puntos? Lo consulta el mostrador antes de atribuir,
// para no discutir a ciegas cuando un cliente reclama.
app.get('/rewards/atribucion', async (req, res) => {
  const staff = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staff)) return;
  const n = parseInt(req.query.n, 10);
  if (!Number.isFinite(n)) return res.status(400).json({ ok: false, error: 'falta n (numero de orden)' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'atribuciones deshabilitadas' });
  try {
    const r = await fetch(REWARDS_SHEETS_URL + '?action=atribucion&order_number=' + n, { redirect: 'follow' });
    const j = await r.json().catch(() => null);
    if (!j) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger' });
    return res.json(j);
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'error consultando la atribucion' });
  }
});

// ── GET /rewards/folio?f=RWD-...&pin= ───────────────────────
// Para staff (F1.5): consulta el estado de un folio de canje en el Ledger
// (Apps Script doGet action=folio). No toca Booqable. El .gs responde
// {ok, found, folio:{folio,fecha,customer_id,email,nombre,reward,points,
//  discount_pct,estado,orden_aplicada}}.
app.get('/rewards/folio', async (req, res) => {
  const staffFolio = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffFolio)) return;
  const folio = String(req.query.f || '').trim().toUpperCase();
  if (folio.indexOf('RWD-') !== 0 || folio.length < 6) {
    return res.status(400).json({ ok: false, error: 'folio invalido (esperado RWD-...)' });
  }
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'consulta de folios deshabilitada (Ledger no configurado)' });
  try {
    const r = await fetch(REWARDS_SHEETS_URL + '?action=folio&folio=' + encodeURIComponent(folio), { redirect: 'follow' });
    if (!r.ok) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger, intenta de nuevo' });
    const j = await r.json().catch(() => null);
    if (!j || j.ok === false) return res.status(502).json({ ok: false, error: 'no se pudo leer el Ledger, intenta de nuevo' });
    if (!j.found) return res.status(404).json({ ok: false, found: false, error: 'folio no encontrado' });
    console.log('[rewards] folio ' + folio + ' -> ' + ((j.folio || {}).estado || '?'));
    return res.json({ ok: true, found: true, folio: j.folio });
  } catch (e) {
    console.error('[rewards] folio error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error consultando el folio, intenta de nuevo' });
  }
});

// ── POST /rewards/folio/aplicar  {folio, order_number, staff_name?, pin?} ──
// Para staff (F1.5): marca un folio de canje como aplicado a una orden.
// POST al Apps Script {tipo:'aplicar', folio, order_number, staff_name};
// el .gs responde {ok, updated:bool, estado_previo}.
app.post('/rewards/folio/aplicar', async (req, res) => {
  const body = req.body || {};
  const staffAplicar = await rewardsStaffFrom(req);
  if (rewardsStaffDenied(res, staffAplicar)) return;
  const folio = String(body.folio || '').trim().toUpperCase();
  const orderNumber = String(body.order_number || '').trim();
  if (folio.indexOf('RWD-') !== 0 || folio.length < 6) {
    return res.status(400).json({ ok: false, error: 'folio invalido (esperado RWD-...)' });
  }
  if (!orderNumber) return res.status(400).json({ ok: false, error: 'order_number requerido' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'aplicacion de folios deshabilitada (Ledger no configurado)' });
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tipo: 'aplicar',
        folio: folio,
        order_number: orderNumber,
        staff_name: staffAplicar   // identidad verificada, no el body
      }),
      redirect: 'follow'
    });
    if (!r.ok) return res.status(502).json({ ok: false, error: 'no se pudo escribir al Ledger, intenta de nuevo' });
    const j = await r.json().catch(() => null);
    if (!j) return res.status(502).json({ ok: false, error: 'respuesta invalida del Ledger, intenta de nuevo' });
    if (j.ok === false) {
      return res.status(404).json({ ok: false, error: j.error || 'folio no encontrado' });
    }
    if (!j.updated) {
      const prev = j.estado_previo || 'desconocido';
      return res.status(409).json({
        ok: false,
        updated: false,
        estado_previo: prev,
        error: prev === 'aplicado' ? 'el folio ya estaba aplicado' : ('el folio no se pudo aplicar (estado: ' + prev + ')')
      });
    }
    console.log('[rewards] folio ' + folio + ' aplicado a orden ' + orderNumber +
      ' por ' + staffAplicar);
    return res.json({ ok: true, updated: true, estado_previo: j.estado_previo || 'pendiente' });
  } catch (e) {
    console.error('[rewards] folio/aplicar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el folio, intenta de nuevo' });
  }
});


// =====================================================================
// v8.9: BORRADOR DE ORDEN desde la conversacion (copiloto del equipo)
// POST /webhook/draft-order  body: { contactId }
// Disparado por un Shortcut en Respond.io. Lee la conversacion, extrae
// con Claude el equipo y las fechas, crea una orden BORRADOR en Booqable
// (status draft — nada le llega al cliente) y deja un comentario interno
// en la conversacion con el numero de orden y lo que falto por matchear.
// Seguridad: si DRAFT_ORDER_TOKEN esta seteada, exige header x-draft-token.
// =====================================================================

const DRAFT_ORDER_TOKEN = process.env.DRAFT_ORDER_TOKEN || '';

// Memoria corta contactId -> {at, numbers} para avisar de posibles duplicados
// cuando alguien aprieta el boton dos veces sobre la misma conversacion.
const draftRecientes = {};

async function draftRespondioGet(path) {
  const r = await fetch('https://api.respond.io/v2' + path, {
    headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' }
  });
  if (!r.ok) throw new Error('Respond.io ' + r.status + ' en ' + path);
  return r.json();
}

async function draftPostComment(contactId, text) {
  try {
    const r = await fetch('https://api.respond.io/v2/contact/id:' + contactId + '/comment', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + RESPONDIO_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: String(text).slice(0, 990) })
    });
    if (!r.ok) console.error('[draft-order] comentario fallo: ' + r.status + ' - ' + (await r.text()));
    return r.ok;
  } catch (e) {
    console.error('[draft-order] comentario error: ' + e.message);
    return false;
  }
}

// Busca un product group por texto y regresa {groupName, productId} o null.
// PROBLEMA CONOCIDO de filter[q] con varias palabras: regresa TODO lo que
// matchee CUALQUIER palabra, en orden alfabetico — para "sony fx3" la camara
// FX3 ni aparece en la primera pagina (salen adaptadores y baterias). Por eso
// juntamos candidatos de VARIAS busquedas (incluida cada palabra-modelo tipo
// "fx3"/"200x" a solas, que si la regresa) y luego exigimos que el nombre
// contenga las palabras pedidas como PALABRA COMPLETA ("grand" != "Grande",
// "fx3" != "FX30"). Sin match confiable regresa null: mejor "agregar a mano"
// que adivinar un producto equivocado.
async function draftFindProduct(query) {
  query = String(query || '').replace(/\([^)]*\)/g, ' ').replace(/["']/g, ' ');
  const norm = function (s) {
    return String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  };
  const contienePalabra = function (texto, w) {
    const esc = w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    if (new RegExp('(^|[^a-z0-9])' + esc + '($|[^a-z0-9])').test(texto)) return true;
    return w.length >= 6 && texto.replace(/\s+/g, '').indexOf(w) !== -1;
  };
  const genericas = ['luz', 'luces', 'lampara', 'camara', 'lente', 'equipo', 'kit', 'de', 'para', 'con',
    'el', 'la', 'un', 'una', 'unos', 'unas', 'dos', 'tres', 'cuatro', 'cinco', 'par'];
  const words = norm(query).split(/\s+/).filter(function (w) { return w.length > 1; });
  if (!words.length) return null;
  const significativas = words.filter(function (w) { return genericas.indexOf(w) === -1; });
  const modelos = significativas.filter(function (w) { return /\d/.test(w); });

  // Pool de candidatos: varias busquedas, deduplicadas por id.
  const queries = [words.join(' ')];
  if (significativas.length && significativas.length < words.length) queries.push(significativas.join(' '));
  modelos.forEach(function (w) { queries.push(w); });
  if (significativas.length > 2) queries.push(significativas.slice(0, 2).join(' '));
  const vistos = {};
  const candidatos = [];
  for (const q of queries) {
    if (!q || vistos['q:' + q]) continue;
    vistos['q:' + q] = 1;
    let d;
    try {
      d = await booqableGet('/product_groups?filter[q]=' + encodeURIComponent(q) + '&page[size]=50');
    } catch (e) { continue; }
    (d.data || []).forEach(function (x) {
      if (x.attributes && !x.attributes.archived && !vistos[x.id]) {
        vistos[x.id] = 1;
        candidatos.push(x);
      }
    });
  }
  if (!candidatos.length) return null;

  // Exigencia por niveles: todas las palabras -> las significativas ->
  // las primeras 2 significativas -> solo el modelo (fx3, 200x).
  // El cliente separa lo que el catalogo junta: "Sony A7 IV" vs "Camara Sony A7IV".
  // Se pegan los tokens cortos contiguos cuando alguno trae digito (a7+iv -> a7iv),
  // asi "a7iv" si matchea como palabra completa (y sigue sin matchear "A7III").
  const pegarModelo = function (ws) {
    const out = [];
    for (let i = 0; i < ws.length; i++) {
      const a = ws[i], b = ws[i + 1];
      // Solo modelo+sufijo: el token traer digito ("a7", "fx3") y el siguiente
      // ser un sufijo corto SIN digito ("iv", "iii", "s"). Asi no pega "sony"+"a7".
      if (b && /\d/.test(a) && !/\d/.test(b) && b.length <= 3) {
        out.push(a + b); i++;
      } else out.push(a);
    }
    return out;
  };
  const wordsPegados = pegarModelo(words);
  const significativasPegadas = pegarModelo(significativas);
  const niveles = [words];
  if (wordsPegados.join(' ') !== words.join(' ')) niveles.push(wordsPegados);
  if (significativasPegadas.join(' ') !== significativas.join(' ')) niveles.push(significativasPegadas);
  if (significativas.length && significativas.join(' ') !== words.join(' ')) niveles.push(significativas);
  if (significativas.length > 2) niveles.push(significativas.slice(0, 2));
  if (modelos.length && modelos.length < significativas.length) niveles.push(modelos);
  for (const setPalabras of niveles) {
    if (!setPalabras.length) continue;
    const exactos = candidatos.filter(function (x) {
      const n = norm(x.attributes.name);
      return setPalabras.every(function (w) { return contienePalabra(n, w); });
    });
    if (!exactos.length) continue;
    exactos.sort(function (a, b) { return a.attributes.name.length - b.attributes.name.length; });
    const g = exactos[0];
    try {
      const pd = await booqableGet('/products?filter[product_group_id]=' + g.id + '&page[size]=1');
      const p = (pd.data || [])[0];
      if (p) return { groupName: g.attributes.name, productId: p.id };
    } catch (e) { /* siguiente nivel */ }
  }
  return null;
}

app.post('/webhook/draft-order', async (req, res) => {
  try {
    if (DRAFT_ORDER_TOKEN && (req.headers['x-draft-token'] || '') !== DRAFT_ORDER_TOKEN) {
      return res.status(401).json({ ok: false, error: 'token invalido' });
    }
    if (!BOOQABLE_API_KEY) {
      return res.status(500).json({ ok: false, error: 'BOOQABLE_API_KEY no configurada' });
    }
    const contactId = extractContactId(req.body);
    if (!contactId || !/^\d+$/.test(String(contactId))) {
      return res.status(400).json({ ok: false, error: 'contactId faltante o invalido' });
    }

    console.log('[draft-order] solicitud para contacto ' + contactId);

    // 1) Contacto + mensajes recientes
    const [contact, messagesData] = await Promise.all([
      draftRespondioGet('/contact/id:' + contactId),
      draftRespondioGet('/contact/id:' + contactId + '/message/list?limit=60')
    ]);
    const msgs = (messagesData.items || messagesData.data || []).slice().reverse(); // cronologico
    // Marcar explicitamente cuando Filmorent ya mando una cotizacion/orden en PDF:
    // todo lo pedido ANTES de eso ya se atendio y no debe volver a convertirse en
    // borrador (asi es como el copiloto revivia rentas viejas ya cotizadas).
    let idxUltimoDoc = -1;
    const filas = msgs.map(function (m, i) {
      const who = m.traffic === 'incoming' ? 'CLIENTE' : 'FILMORENT';
      const msg = m.message || {};
      let t = msg.text;
      if (!t && msg.type === 'attachment') {
        const att = msg.attachment || {};
        const nombre = String(att.fileName || '').toLowerCase();
        const esDoc = att.type === 'file' || /invoice|cotiza|orden|pro_forma|proforma|\.pdf/.test(nombre);
        if (who === 'FILMORENT' && esDoc) {
          idxUltimoDoc = i;
          t = '[FILMORENT LE ENVIO UN DOCUMENTO (cotizacion/orden): ' + (att.fileName || 'documento') + ']';
        } else {
          t = '[' + (att.type || 'adjunto') + ']';
        }
      }
      if (!t) t = '[' + (msg.type || 'adjunto') + ']';
      return who + ': ' + String(t).replace(/\s+/g, ' ').slice(0, 300);
    });
    if (idxUltimoDoc >= 0 && idxUltimoDoc < filas.length - 1) {
      filas.splice(idxUltimoDoc + 1, 0,
        '--- TODO LO DE ARRIBA YA SE ATENDIO (Filmorent ya envio el documento). ' +
        'SOLO lo de aqui para abajo puede estar pendiente. ---');
    }
    const transcript = filas.join('\n').slice(-8000);

    if (!transcript) {
      return res.status(422).json({ ok: false, error: 'conversacion vacia' });
    }

    const nombreContacto = (((contact.firstName || '') + ' ' + (contact.lastName || '')).trim()) || null;
    const phone = String(contact.phone || '').replace(/[^0-9]/g, '');

    // 2) Claude extrae equipo y fechas
    // 4) Cliente en Booqable.
    // Un mismo humano suele tener VARIOS registros (el suyo y el de su empresa;
    // ej. tel 818254xxxx -> "Pasumecha Producciones" 79 ordenes y "Daniel Alonso"
    // 59 ordenes). Se juntan candidatos por TELEFONO y por NOMBRE, con su
    // historial, y solo se asigna solo cuando hay UNO. Si hay varios, se listan
    // con historial y se genera la pregunta para el cliente.
    const soloDigitos = function (s) { return String(s || '').replace(/[^0-9]/g, ''); };
    const normTxt = function (s) {
      return String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    };
    // Con UNA palabra compartida entraba mucho ruido ("Daniel Alonso" matcheaba
    // "Alfredo Lamas Daniel" y "Joseph Daniel Shay"). Si el contacto tiene nombre
    // y apellido, se exigen AL MENOS DOS palabras en comun.
    const compartePalabra = function (a, b) {
      const nb = normTxt(b);
      const ws = normTxt(a).split(/\s+/).filter(function (w) { return w.length > 2; });
      if (!ws.length) return false;
      const hits = ws.filter(function (w) { return nb.indexOf(w) !== -1; }).length;
      return ws.length >= 2 ? hits >= 2 : hits >= 1;
    };
    const resumeCliente = function (c) {
      const a = c.attributes;
      return {
        id: c.id,
        name: a.name,
        ordenes: a.order_count || 0,
        ultima: (a.latest_order_at || '').slice(0, 10),
        porTelefono: false
      };
    };
    const candidatos = [];
    const yaVisto = {};
    const phone10 = phone.slice(-10);
    if (phone10.length === 10) {
      try {
        const cd = await booqableGet('/customers?filter[q]=' + phone10 + '&page[size]=5');
        (cd.data || []).forEach(function (c) {
          const p = (c.attributes && c.attributes.properties) || {};
          const cuadra = [p.phone, p.phone_2].some(function (t) {
            const d = soloDigitos(t);
            return d && d.endsWith(phone10);
          });
          if (cuadra && !yaVisto[c.id]) {
            yaVisto[c.id] = 1;
            const r = resumeCliente(c);
            r.porTelefono = true;
            candidatos.push(r);
          }
        });
      } catch (e) { /* sin candidatos por telefono */ }
    }
    if (nombreContacto) {
      try {
        const cd = await booqableGet('/customers?filter[q]=' + encodeURIComponent(nombreContacto) + '&page[size]=5');
        (cd.data || []).forEach(function (c) {
          if (yaVisto[c.id] || !c.attributes) return;
          // filter[q] por nombre es difuso: exigir que comparta palabra Y que
          // tenga historial real, para no sugerir homonimos al azar.
          if (!compartePalabra(nombreContacto, c.attributes.name)) return;
          if (!(c.attributes.order_count > 0)) return;
          yaVisto[c.id] = 1;
          candidatos.push(resumeCliente(c));
        });
      } catch (e) { /* sin candidatos por nombre */ }
    }
    candidatos.sort(function (a, b) { return (b.ultima || '').localeCompare(a.ultima || ''); });

    let customerId = null;
    let customerName = null;
    if (candidatos.length === 1 && candidatos[0].porTelefono &&
        (!nombreContacto || compartePalabra(nombreContacto, candidatos[0].name))) {
      customerId = candidatos[0].id;
      customerName = candidatos[0].name;
    }


    // Que ya tiene pedido este cliente en BOOQABLE (la fuente de verdad, no el
    // chat): sin esto el copiloto volvia a crear ordenes de cosas que el equipo
    // ya habia levantado. Se consultan las ordenes vigentes de los candidatos.
    const hoyIso = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });
    const desdeIso = new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString().slice(0, 10);
    // Fuentes: (a) las ordenes de los clientes candidatos y (b) las ordenes
    // etiquetadas con ESTA conversacion. La (b) es indispensable: cuando el
    // cliente queda sin asignar (persona vs empresa), la orden no aparece por
    // customer_id y el copiloto la volvia a crear.
    const tagConversacion = 'wa-' + contactId;
    const ordenesRelevantes = {};
    const consultas = candidatos.slice(0, 2).map(function (c) {
      return { url: '/orders?filter[customer_id]=' + c.id + '&sort=-number&page[size]=8', quien: c.name };
    });
    consultas.push({
      url: '/orders?filter[tag_list]=' + encodeURIComponent(tagConversacion) + '&sort=-number&page[size]=8',
      quien: 'esta conversacion'
    });
    for (const q of consultas) {
      let od;
      try { od = await booqableGet(q.url); } catch (e) { continue; }
      for (const o of (od.data || [])) {
        const a = o.attributes;
        if (!a || a.status === 'canceled') continue;
        if ((a.stops_at || '').slice(0, 10) < desdeIso) continue;
        if (!ordenesRelevantes[o.id]) ordenesRelevantes[o.id] = { o: o, quien: q.quien };
      }
    }
    const yaPedido = [];
    const ordenesVigentes = [];
    for (const k of Object.keys(ordenesRelevantes)) {
      const o = ordenesRelevantes[k].o;
      const a = o.attributes;
      let items = [];
      try {
        const ld = await booqableGet('/lines?filter[owner_id]=' + o.id + '&page[size]=25');
        items = (ld.data || [])
          .filter(function (l) { return l.attributes && l.attributes.item_id; })
          .map(function (l) { return (l.attributes.title || '').trim(); })
          .filter(Boolean);
      } catch (e) { /* sin detalle */ }
      yaPedido.push('#' + a.number + ' (' + a.status + ') ' + (a.starts_at || '').slice(0, 10) +
        ' a ' + (a.stops_at || '').slice(0, 10) + ': ' +
        (items.length ? items.slice(0, 12).join(', ') : 'sin equipo capturado'));
      ordenesVigentes.push({
        number: a.number, status: a.status,
        fi: (a.starts_at || '').slice(0, 10), ff: (a.stops_at || '').slice(0, 10),
        items: items
      });
    }
    const bloqueYaPedido = yaPedido.length
      ? '\nORDENES QUE ESTE CLIENTE YA TIENE EN EL SISTEMA (NO las vuelvas a crear):\n' +
        yaPedido.slice(0, 8).join('\n') + '\n'
      : '';

    const hoyMty = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });
    const extractPrompt = 'Eres el asistente interno de Filmorent (renta de equipo audiovisual en Monterrey). ' +
      'Lee esta conversacion de WhatsApp y arma los BORRADORES de orden que hagan falta.\n\n' +
      'HOY es ' + hoyMty + ' (zona America/Monterrey).\n\n' +
      'CONVERSACION (CLIENTE = el cliente, FILMORENT = nuestro equipo o el bot):\n' + transcript + '\n' +
      bloqueYaPedido + '\n' +
      'Regresa UNICAMENTE un objeto JSON valido, sin markdown ni texto extra:\n' +
      '{"solicitudes":[{' +
      '"equipos":[{"descripcion":"nombre CORTO, MAXIMO 5 palabras, SIN parentesis (los detalles van en notas), ej: Sony FX3, DJI Mini 4 Pro, luz Amaran 200x. Para estudios usa exactamente: Estudio Filmo Grand, Estudio Filmo Pocket o Estudio Podcast","cantidad":1}],' +
      '"fecha_inicio":"YYYY-MM-DD dia que EMPIEZA a usar, o null",' +
      '"fecha_regreso":"YYYY-MM-DD dia que REGRESA (si solo dijo hasta cuando lo USA, el regreso es la manana del dia siguiente), o null",' +
      '"hora_inicio":"HH:MM SOLO si dijo a que hora recoge, si no null",' +
      '"hora_regreso":"HH:MM SOLO si dijo a que hora regresa, si no null"' +
      '}],' +
      '"notas":"detalles utiles para el equipo (proyecto, dudas abiertas, entrega); breve",' +
      '"confianza":"alta|media|baja"}\n\n' +
      'REGLA MAS IMPORTANTE — UNA SOLICITUD POR RANGO DE FECHAS:\n' +
      'Si el cliente pidio cosas para DIAS DISTINTOS, son solicitudes SEPARADAS, NO las juntes.\n' +
      'Ejemplo: "dos FX3 para manana" + "una FX6 para el miercoles" = DOS solicitudes.\n' +
      'Un estudio por bloques ("3 dias de 4 horas cada dia") = UNA solicitud POR DIA, cada una con su horario.\n' +
      'Solo van juntos en la MISMA solicitud los equipos que se recogen y regresan en las MISMAS fechas.\n\n' +
      'QUE INCLUIR: SOLO lo que NO tiene orden todavia. Si arriba aparece "ORDENES QUE ESTE CLIENTE ' +
      'YA TIENE", eso YA ESTA LEVANTADO: NO lo repitas aunque se hable de ello en el chat ' +
      '(compara equipo Y fechas). Si en la conversacion aparece la linea ' +
      '"TODO LO DE ARRIBA YA SE ATENDIO", ignora por completo lo que este ARRIBA de esa linea: ' +
      'ya se cotizo y ya se le mando. Tampoco incluyas lo que solo pregunto por precio y descarto. ' +
      'Ante la duda de si algo ya se atendio, DEJALO FUERA: es mas facil que el equipo agregue una ' +
      'linea que darse cuenta de que sobra una orden.\n' +
      'Fechas relativas ("este viernes", "el fin") se calculan con la fecha de HOY. cantidad default 1. ' +
      'Si no hay nada pendiente que rentar, solicitudes = [].';

    // Extraccion con reintento: si la respuesta viene vacia o el JSON no parsea,
    // se intenta una segunda vez con instruccion mas dura. Cualquier fallo se le
    // AVISA al equipo en la conversacion — nunca fallar en silencio (el peor bug:
    // el empleado aprieta el boton y no pasa nada).
    const parseExt = function (raw) {
      if (!raw) return null;
      const ini = raw.indexOf('{');
      const fin = raw.lastIndexOf('}');
      if (ini === -1 || fin <= ini) return null;
      try { return JSON.parse(raw.slice(ini, fin + 1)); } catch (e) { return null; }
    };
    let ext = null;
    let ultimoRaw = '';
    for (let intento = 1; intento <= 2 && !ext; intento++) {
      const contenido = intento === 1
        ? extractPrompt
        : extractPrompt + '\n\nIMPORTANTE: tu respuesta anterior no fue JSON valido. ' +
          'Responde SOLO el objeto JSON, empezando con { y terminando con }. Sin texto antes ni despues.';
      // CAUSA RAIZ MEDIDA (30-jul, log de Render): el intento fallaba con
      // stop_reason=max_tokens y bloques "thinking,text" — el razonamiento
      // adaptativo se comia el presupuesto y truncaba el JSON. Esta extraccion
      // no necesita pensar, asi que en el 1er intento se apaga. El 2o intento va
      // SIN el parametro: si algun dia el modelo no lo acepta, se auto-repara.
      const params = { model: 'claude-sonnet-5', messages: [{ role: 'user', content: contenido }] };
      if (intento === 1) {
        params.max_tokens = 2000;
        params.thinking = { type: 'disabled' };
      } else {
        params.max_tokens = 8000;
      }
      let resp;
      try {
        resp = await anthropic.messages.create(params);
      } catch (e) {
        console.error('[draft-order] error de API en intento ' + intento + ': ' + e.message);
        continue;
      }
      ultimoRaw = claudeText(resp);
      console.log('[draft-order] intento ' + intento + ': stop_reason=' + (resp.stop_reason || '?') +
        ' bloques=' + ((resp.content || []).map(function (b) { return b.type; }).join(',') || 'ninguno') +
        ' chars=' + ultimoRaw.length);
      ext = parseExt(ultimoRaw);
    }
    if (!ext) {
      console.error('[draft-order] JSON ilegible tras 2 intentos. Raw: ' + ultimoRaw.slice(0, 400));
      await draftPostComment(contactId,
        '\ud83e\udd16 No pude armar el borrador: no logre entender la conversacion (fallo tecnico de lectura). ' +
        'Vuelve a apretar el boton, o crea la orden a mano en Booqable. Ya quedo registrado para revisarlo.');
      return res.status(422).json({ ok: false, error: 'no se pudo extraer datos de la conversacion' });
    }
    // Compatibilidad: si el modelo regresa el formato viejo {equipos:[...]},
    // se envuelve como una sola solicitud.
    let solicitudes = Array.isArray(ext.solicitudes) ? ext.solicitudes : null;
    if (!solicitudes && Array.isArray(ext.equipos)) {
      solicitudes = [{
        equipos: ext.equipos, fecha_inicio: ext.fecha_inicio, fecha_regreso: ext.fecha_regreso,
        hora_inicio: ext.hora_inicio, hora_regreso: ext.hora_regreso
      }];
    }
    if (!solicitudes) {
      console.error('[draft-order] extraccion sin solicitudes');
      return res.status(422).json({ ok: false, error: 'extraccion incompleta, reintenta' });
    }
    solicitudes = solicitudes.filter(function (s) {
      return s && Array.isArray(s.equipos) &&
        s.equipos.filter(function (x) { return x && x.descripcion; }).length;
    }).slice(0, 6);
    if (!solicitudes.length) {
      const l = ['\ud83e\udd16 No cree ningun borrador: no hay nada pendiente en esta conversacion.'];
      if (yaPedido.length) {
        l.push('Lo que pidio ya esta levantado:');
        yaPedido.slice(0, 5).forEach(function (y) { l.push('  ' + y); });
        l.push('Si falta algo o hay que moverle a esas ordenes, hazlo en Booqable.');
      } else {
        l.push('No detecte equipo claro que el cliente quiera rentar. Si falta algo, crealo a mano.');
      }
      await draftPostComment(contactId, l.join('\n'));
      return res.json({ ok: false, error: 'sin equipos pendientes detectados', ya_pedido: yaPedido });
    }

    // 3) Helpers de fechas/horas.
    // Booqable guarda la hora VERBATIM con etiqueta +00:00 (igual que las ordenes
    // que el equipo crea a mano): NO convertir a UTC real.
    // Politica Filmorent: recoleccion 9:00; regreso a la MANANA siguiente 9:30;
    // renta del mismo dia regresa 19:00.
    const validaFecha = function (s) { return (s && /^\d{4}-\d{2}-\d{2}$/.test(s)) ? s : null; };
    const validaHora = function (s) {
      if (!s || !/^\d{1,2}:\d{2}$/.test(s)) return null;
      return s.length === 4 ? '0' + s : s;
    };
    const esDomingo = function (s) { return new Date(s + 'T12:00:00Z').getUTCDay() === 0; };
    const manana = new Date(Date.now() + 24 * 3600 * 1000)
      .toLocaleDateString('en-CA', { timeZone: 'America/Monterrey' });

    // 5) Una ORDEN POR SOLICITUD (fechas distintas = ordenes distintas).
    const patchAttrsBase = { tag_list: ['borrador-ai', tagConversacion] };
    if (customerId) patchAttrsBase.customer_id = customerId;
    const resultados = [];
    let patchFallo = false;
    const conflictos = [];
    for (const sol of solicitudes) {
      const equipos = sol.equipos.filter(function (x) { return x && x.descripcion; });
      let fi = validaFecha(sol.fecha_inicio);
      let ff = validaFecha(sol.fecha_regreso);
      const fechasAsumidas = !fi;
      if (!fi) fi = manana;
      const hIni = validaHora(sol.hora_inicio) || '09:00';
      let hReg = validaHora(sol.hora_regreso);
      if (!ff || ff < fi) {
        // Criterio de Barush (28-jul): el regreso normal es a la MANANA SIGUIENTE
        // 9:00-9:30. Solo se queda el mismo dia si el cliente dio hora de regreso
        // (tipico de estudios rentados por horas).
        if (hReg) {
          ff = fi;
        } else {
          ff = new Date(fi + 'T12:00:00Z');
          ff.setUTCDate(ff.getUTCDate() + 1);
          ff = ff.toISOString().slice(0, 10);
          hReg = '09:30';
        }
      }
      if (!hReg) hReg = ff > fi ? '09:30' : '19:00';

      // Resolver productos ANTES de crear nada: asi se puede comparar contra lo
      // que el cliente YA tiene y no se crean ordenes vacias.
      const resueltos = [];
      const noEncontrados = [];
      const omitidos = Math.max(0, equipos.length - 12);
      for (const eq of equipos.slice(0, 12)) {
        const qty = Math.max(1, parseInt(eq.cantidad, 10) || 1);
        const hit = await draftFindProduct(eq.descripcion);
        if (!hit) { noEncontrados.push(eq.descripcion); continue; }
        resueltos.push({ pedido: eq.descripcion, qty: qty, hit: hit });
      }

      // Choque con una orden existente: fechas que se traslapan + mismo producto.
      // No se decide solo: se le pregunta al equipo (idea de Daniel).
      const choques = [];
      for (const r of resueltos) {
        const ya = ordenesVigentes.find(function (v) {
          const seTraslapan = v.fi <= ff && v.ff >= fi;
          return seTraslapan && v.items.some(function (t) { return t === r.hit.groupName; });
        });
        if (ya) choques.push({ producto: r.hit.groupName, orden: ya });
      }
      if (choques.length && choques.length === resueltos.length) {
        conflictos.push({
          fi: fi, ff: ff,
          productos: choques.map(function (c) { return c.producto; }),
          ordenes: Array.from(new Set(choques.map(function (c) { return c.orden.number; })))
        });
        continue; // no se crea nada hasta que el equipo aclare
      }

      let orderId;
      try {
        const od = await booqableWrite('POST', '/orders', {
          data: {
            type: 'orders',
            attributes: { starts_at: fi + 'T' + hIni + ':00+00:00', stops_at: ff + 'T' + hReg + ':00+00:00' }
          }
        });
        orderId = od.data.id;
      } catch (e) {
        console.error('[draft-order] no se pudo crear la orden: ' + e.message);
        continue;
      }

      const agregados = [];
      const fallaronReserva = [];
      for (const r of resueltos) {
        try {
          await booqableWrite('POST', '/order_fulfillments', {
            data: {
              type: 'order_fulfillments',
              attributes: {
                order_id: orderId,
                actions: [{ action: 'book_product', mode: 'create_new', product_id: r.hit.productId, quantity: r.qty }]
              }
            }
          });
          agregados.push(r.pedido + ' -> ' + r.hit.groupName + (r.qty > 1 ? ' x' + r.qty : ''));
        } catch (e) {
          console.error('[draft-order] book ' + r.hit.groupName + ': ' + e.message);
          fallaronReserva.push(r.pedido + ' (' + r.hit.groupName + ')');
        }
      }

      try {
        await booqableWrite('PATCH', '/orders/' + orderId, {
          data: { type: 'orders', id: orderId, attributes: patchAttrsBase }
        });
      } catch (e) {
        patchFallo = true;
        console.error('[draft-order] patch cliente/tag: ' + e.message);
      }

      let orderNumber = null;
      try {
        await booqableWrite('POST', '/order_status_transitions', {
          data: {
            type: 'order_status_transitions',
            attributes: { order_id: orderId, transition_from: 'new', transition_to: 'draft' }
          }
        });
        const chk = await booqableGet('/orders/' + orderId);
        orderNumber = chk.data.attributes.number;
      } catch (e) { console.error('[draft-order] transicion a draft: ' + e.message); }

      resultados.push({
        orderId: orderId, number: orderNumber, agregados: agregados, noEncontrados: noEncontrados,
        fallaronReserva: fallaronReserva, omitidos: omitidos, totalPedido: equipos.length,
        parcialmenteRepetida: choques.length ? choques.map(function (c) { return c.producto + ' ya esta en #' + c.orden.number; }) : [],
        fi: fi, ff: ff, hIni: hIni, hReg: hReg, fechasAsumidas: fechasAsumidas,
        sinHora: !validaHora(sol.hora_inicio) && !fechasAsumidas,
        domingo: esDomingo(fi) || esDomingo(ff)
      });
    }

    // Si TODO lo que pidio ya existe, no se crea nada: se pregunta.
    if (!resultados.length && conflictos.length) {
      const l = ['\ud83e\udd16 NO cree ningun borrador para evitar duplicados.'];
      conflictos.forEach(function (c) {
        l.push('Ya existe la orden #' + c.ordenes.join(', #') + ' con ' + c.productos.join(', ') +
          ' en esas mismas fechas (' + c.fi + ' a ' + c.ff + ').');
      });
      l.push('');
      l.push('PREGUNTALE AL CLIENTE (copia y pega):');
      l.push('  Ya tienes una orden apartada para esas fechas con lo mismo. \u00bfEs una orden NUEVA, ' +
        'es la misma, o quieres que le movamos a la que ya tienes?');
      l.push('');
      l.push('Si es NUEVA: duplica la orden en Booqable o crea otra a mano.');
      await draftPostComment(contactId, l.join('\n'));
      return res.json({ ok: true, ordenes: [], conflictos: conflictos });
    }

    if (!resultados.length) {
      await draftPostComment(contactId,
        '\ud83e\udd16 No pude crear el borrador en Booqable (error al crear la orden). Hazlo a mano por favor.');
      return res.status(502).json({ ok: false, error: 'no se pudo crear ninguna orden' });
    }

    // 6) UN comentario con todo. Incluye aviso de posible duplicado.
    // El comentario se arma en dos partes y las PREGUNTAS van HASTA ARRIBA:
    // Respond.io colapsa los comentarios largos con "Show more" y lo que queda
    // abajo no se ve (Daniel: "hizo la orden pero sin preguntarme a que nombre"
    // — si preguntaba, pero escondido).
    const preguntas = [];
    const cabeza = [];
    const detalle = [];
    const previo = draftRecientes[contactId];
    const ahora = Date.now();
    cabeza.push(resultados.length === 1
      ? '🤖 BORRADOR ' + (resultados[0].number ? '#' + resultados[0].number : '(sin numero)') +
        ' creado. NADA se envio al cliente.'
      : '🤖 ' + resultados.length + ' BORRADORES creados (fechas distintas = ordenes distintas). ' +
        'NADA se envio al cliente.');
    draftRecientes[contactId] = {
      at: ahora,
      numbers: resultados.map(function (r) { return r.number || '?'; })
    };

    let lineaCliente;
    if (customerName && !patchFallo) {
      lineaCliente = 'Cliente: ' + customerName + ' (' + (candidatos[0] ? candidatos[0].ordenes : '?') +
        ' rentas previas). Confirma que va a ESTE cliente antes de enviar.';
    } else if (customerName && patchFallo) {
      lineaCliente = '⚠️ Cliente: NO SE PUDO ASIGNAR por un error tecnico. Deberia ser "' +
        customerName + '" — asignalo a mano (y ponle el tag borrador-ai).';
    } else if (candidatos.length > 1) {
      lineaCliente = 'Cliente: SIN ASIGNAR — este contacto tiene ' + candidatos.length +
        ' registros en Booqable: ' + candidatos.slice(0, 4).map(function (c) {
          return c.name + ' (' + c.ordenes + ' rentas, ultima ' + (c.ultima || 's/f') + ')';
        }).join(' | ');
      preguntas.push('¿La orden va a nombre de ' + candidatos[0].name + ' o de ' + candidatos[1].name + '?');
    } else if (candidatos.length === 1) {
      lineaCliente = 'Cliente: SIN ASIGNAR. Unico parecido: "' + candidatos[0].name + '" (' +
        candidatos[0].ordenes + ' rentas, ultima ' + (candidatos[0].ultima || 's/f') + ')' +
        (candidatos[0].porTelefono ? ' — su telefono coincide pero el nombre no; puede ser su empresa.' : '.');
      preguntas.push('¿La orden va a nombre de ' + candidatos[0].name + '?');
    } else {
      lineaCliente = 'Cliente: SIN ASIGNAR, no lo encontre en Booqable' +
        (nombreContacto ? ' (en WhatsApp se llama "' + nombreContacto + '")' : '') + '. Crealo o buscalo tu.';
      preguntas.push('¿A nombre de quien facturamos la orden?');
    }

    for (const r of resultados) {
      detalle.push('');
      detalle.push((r.number ? '#' + r.number : '(sin numero)') + ' — recoge ' + r.fi + ' ' + r.hIni +
        ', regresa ' + r.ff + ' ' + r.hReg + (r.fechasAsumidas ? ' (FECHAS ASUMIDAS)' : ''));
      if (r.agregados.length) detalle.push('  Equipo: ' + r.agregados.join(' | '));
      if (r.parcialmenteRepetida && r.parcialmenteRepetida.length) {
        detalle.push('  ⚠️ Repetido: ' + r.parcialmenteRepetida.join('; '));
        preguntas.push('Ya tienes apartado ' + r.parcialmenteRepetida[0].split(' ya esta')[0] +
          ' para esas fechas. ¿Es una orden NUEVA, es la misma, o le movemos a la que ya tienes?');
      }
      if (r.noEncontrados.length) detalle.push('  ⚠️ NO encontre en catalogo: ' + r.noEncontrados.join(', '));
      if (r.fallaronReserva.length) detalle.push('  ⚠️ Encontrados pero NO agregados: ' + r.fallaronReserva.join(', '));
      if (r.omitidos > 0) detalle.push('  ⚠️ Pidio ' + r.totalPedido + ' equipos; solo procese 12.');
      if (r.sinHora) {
        detalle.push('  ⚠️ Sin hora de recoleccion (se asumio 9:00).');
        preguntas.push('¿A que hora pasas por el equipo el ' + r.fi + '? ¿Y que dia lo regresas?');
      }
      if (r.domingo) detalle.push('  ⚠️ Cae en DOMINGO (cerrado) — ajustar.');
      detalle.push('  https://filmorent-sa-de-cv.booqable.com/orders/' + r.orderId);
    }

    const hayEstudio = resultados.some(function (r) {
      return r.agregados.some(function (a) { return /estudio/i.test(a); });
    });
    if (hayEstudio) {
      preguntas.push('¿Cuantas personas van a estar? ¿Necesitas sala adicional o solo el estudio?');
    }

    const lineas = cabeza.slice();
    if (preguntas.length) {
      lineas.push('');
      lineas.push('❓ FALTA PREGUNTARLE AL CLIENTE (copia y pega):');
      preguntas.slice(0, 4).forEach(function (q) { lineas.push('  ' + q); });
    }
    lineas.push('');
    lineas.push(lineaCliente);
    if (previo && (ahora - previo.at) < 6 * 3600 * 1000) {
      lineas.push('⚠️ Hace ' + Math.round((ahora - previo.at) / 60000) + ' min ya se habia creado ' +
        (previo.numbers.length > 1 ? 'los borradores #' : 'el borrador #') + previo.numbers.join(', #') +
        ' para este contacto.');
    }
    Array.prototype.push.apply(lineas, detalle);
    if (ext.notas) { lineas.push(''); lineas.push('Notas: ' + String(ext.notas).slice(0, 160)); }
    await draftPostComment(contactId, lineas.join('\n'));

    console.log('[draft-order] ' + resultados.length + ' orden(es) ' +
      resultados.map(function (r) { return r.number; }).join(',') + ' para contacto ' + contactId);
    return res.json({
      ok: true,
      ordenes: resultados.map(function (r) {
        return {
          number: r.number, order_id: r.orderId, agregados: r.agregados,
          no_encontrados: r.noEncontrados, fallaron_reserva: r.fallaronReserva,
          recoge: r.fi + ' ' + r.hIni, regresa: r.ff + ' ' + r.hReg, fechas_asumidas: r.fechasAsumidas
        };
      }),
      cliente: customerName || null,
      candidatos: candidatos,
      preguntas: preguntas,
      duplicado_de: previo && (ahora - previo.at) < 6 * 3600 * 1000 ? previo.numbers : null
    });
  } catch (e) {
    console.error('[draft-order] error: ' + (e && e.message));
    // Nunca dejar al empleado esperando sin saber que paso.
    try {
      const cid = extractContactId(req.body);
      if (cid && /^\d+$/.test(String(cid))) {
        await draftPostComment(cid,
          '\ud83e\udd16 No pude crear el borrador por un error tecnico. Crea la orden a mano en Booqable ' +
          'o vuelve a intentar en un minuto. (Detalle: ' + String((e && e.message) || e).slice(0, 120) + ')');
      }
    } catch (e2) { /* ni modo */ }
    return res.status(500).json({ ok: false, error: String((e && e.message) || e).slice(0, 300) });
  }
});


app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v7.2.1 running on port ' + PORT);
  console.log('Whisper transcription: ' + (openai ? 'ENABLED' : 'DISABLED (set OPENAI_API_KEY to enable)'));
  console.log('Auto-summary on conversation opened: ENABLED');
  console.log('Draft-order copilot (/webhook/draft-order): ' + (BOOQABLE_API_KEY ? ('ENABLED, token ' + (DRAFT_ORDER_TOKEN ? 'ON' : 'OFF')) : 'DISABLED (set BOOQABLE_API_KEY)'));
  console.log('Rewards endpoints (/rewards/*): ' + (BOOQABLE_API_KEY ? 'ENABLED' : 'DISABLED (set BOOQABLE_API_KEY)') + (REWARDS_SHEETS_URL ? ', ledger ON' : ', ledger OFF'));
});
