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
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v8.0', whisper: !!openai, autoSummary: true, rewards: !!BOOQABLE_API_KEY }));

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
 * Extract agents who ACTUALLY SENT messages.
 * Uses sender.source: "user" = human, "ai_agent" = bot
 */
function extractAgentsFromMessages(messages) {
  const humanAgents = new Map();
  const botAgents = new Map();

  for (const msg of messages) {
    const traffic = msg.traffic || msg.type;
    if (traffic !== 'outgoing') continue;

    const sender = msg.sender;
    if (!sender) continue;

    const uid = sender.userId;
    const source = sender.source;

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

  return {
    humans: Array.from(humanAgents.values()),
    bots: Array.from(botAgents.values()),
    all: [...Array.from(botAgents.values()), ...Array.from(humanAgents.values())]
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

    // Extract agents who ACTUALLY sent messages
    const { humans, bots, all: allAgents } = extractAgentsFromMessages(messages);
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
//   - QR determinstico del customer_id, formato FLM-XX-YYYY-XXNX. Hash FNV-1a
//     32-bit (el hash del prototipo — suma de charCodes — solo producia ~30k
//     valores => colisiones casi seguras entre ~2k miembros).
// ============================================================

const BOOQABLE_API_KEY = process.env.BOOQABLE_API_KEY;
const BOOQABLE_BASE = process.env.BOOQABLE_BASE || 'https://filmorent-sa-de-cv.booqable.com/api/4';
const REWARDS_SHEETS_URL = process.env.REWARDS_SHEETS_URL; // Apps Script del Rewards Ledger
const REWARDS_STAFF_PIN = process.env.REWARDS_STAFF_PIN;   // opcional: PIN para /rewards/scan

// Catalogo v1 (solo descuentos). El portal lo consume de aqui — una sola fuente.
const REWARDS_CATALOG = [
  { id: 1, name: '5% descuento en tu próxima renta', points: 100, value: 5 },
  { id: 2, name: '10% descuento en tu próxima renta', points: 250, value: 10 },
  { id: 3, name: '15% descuento en tu próxima renta', points: 500, value: 15 },
  { id: 4, name: '20% descuento en tu próxima renta', points: 800, value: 20 },
  { id: 5, name: '25% descuento en tu próxima renta', points: 1200, value: 25 }
];

const REWARDS_TIERS = [
  { name: 'Bronce', min: 0, max: 499, discount: 0 },
  { name: 'Plata', min: 500, max: 1499, discount: 5 },
  { name: 'Oro', min: 1500, max: null, discount: 10 }
];

// CORS solo para /rewards/* (el portal vive en otro dominio).
const REWARDS_ORIGINS = [
  'https://rewards.filmorent.com',
  'https://filmorent.com',
  'https://www.filmorent.com'
];
// Rate limit simple por IP (el endpoint es publico y devuelve datos de miembro):
// 30 requests por ventana de 5 min. En memoria — suficiente para un solo dyno.
const rewardsRate = new Map();
// IP real del cliente (x-forwarded-for primero — Render corre detras de proxy)
// y user-agent recortado. Los usan el rate limiter y el audit trail del Ledger.
function rewardsClientIp(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.ip || 'desconocida';
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
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

function rewardsTierFor(points) {
  for (let i = REWARDS_TIERS.length - 1; i >= 0; i--) {
    if (points >= REWARDS_TIERS[i].min) return REWARDS_TIERS[i];
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
    const st = (o.attributes || {}).status;
    return st !== 'draft' && st !== 'concept' && st !== 'canceled';
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
  const orderRows = countable.map(o => {
    const a = o.attributes || {};
    const g = a.grand_total_in_cents || 0;
    const gt = a.grand_total_with_tax_in_cents || 0;
    const elWithTax = elsepcByOrder[o.id] || 0;
    const ratio = gt ? (g / gt) : 1;
    const elBase = Math.min(Math.round(elWithTax * ratio), g);
    totalBaseCents += g;
    totalElsepcCents += elBase;
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
    return { redeemed_points: j.redeemed_points || 0, redemptions: j.redemptions || [] };
  } catch (e) {
    console.error('rewards ledger read error: ' + e.message);
    return null;
  }
}

// Escribe una fila al Ledger (Apps Script doPost). true/false.
async function rewardsLedgerWrite(row) {
  if (!REWARDS_SHEETS_URL) return false;
  try {
    const r = await fetch(REWARDS_SHEETS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(row),
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
  const available = Math.max(0, earned.points_earned - redeemed);
  const tier = rewardsTierFor(available);
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
      avg_order_cents: a.average_order_value_in_cents || 0
    },
    points: {
      earned: earned.points_earned,
      redeemed: redeemed,
      available: available,
      revenue_cents: earned.revenue_cents,
      elsepc_excluded_cents: earned.elsepc_excluded_cents
    },
    tier: { name: tier.name, discount: tier.discount },
    orders: earned.orders,
    redemptions: ledger ? ledger.redemptions : [],
    catalog: REWARDS_CATALOG,
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
  const reward = REWARDS_CATALOG.find(r => r.id === rewardId);
  if (!reward) return res.status(400).json({ ok: false, error: 'recompensa invalida' });
  if (!REWARDS_SHEETS_URL) return res.status(503).json({ ok: false, error: 'canjes temporalmente deshabilitados (Ledger no configurado)' });

  try {
    const customer = await rewardsFindCustomer(email);
    if (!customer) return res.status(404).json({ ok: false, error: 'no existe cuenta con ese email' });

    // Recalcular saldo EN VIVO antes de canjear (no confiar en el cliente).
    const earned = await rewardsComputeEarned(customer.id);
    const ledger = await rewardsLedgerSummary(customer.id);
    if (!ledger) return res.status(503).json({ ok: false, error: 'no se pudo leer el Ledger, intenta mas tarde' });
    const available = Math.max(0, earned.points_earned - ledger.redeemed_points);
    if (available < reward.points) {
      return res.status(409).json({ ok: false, error: 'puntos insuficientes', points_available: available });
    }

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
      reward_name: reward.name,
      puntos: reward.points,
      descuento_pct: reward.value,
      estado: 'pendiente',
      ip: rewardsClientIp(req),
      ua: rewardsClientUa(req)
    });
    if (!wrote) return res.status(502).json({ ok: false, error: 'no se pudo registrar el canje, intenta de nuevo' });

    console.log('[rewards] canje ' + folio + ' ' + email + ' -' + reward.points + ' pts (' + reward.value + '%)');
    return res.json({
      ok: true,
      folio: folio,
      reward: reward,
      points_available: available - reward.points,
      // el Ledger esta configurado (503 arriba si no) y el .gs manda el correo
      // de confirmacion al registrar la fila del canje
      email_confirmacion: true,
      instrucciones: 'Presenta el folio ' + folio + ' al confirmar tu próxima renta para aplicar tu ' + reward.value + '% de descuento.'
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
  if (REWARDS_STAFF_PIN && String(body.pin || '') !== REWARDS_STAFF_PIN) {
    return res.status(401).json({ ok: false, error: 'PIN de staff invalido' });
  }
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
      staff_name: body.staff_name || '',
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

// ── GET /rewards/folio?f=RWD-...&pin= ───────────────────────
// Para staff (F1.5): consulta el estado de un folio de canje en el Ledger
// (Apps Script doGet action=folio). No toca Booqable. El .gs responde
// {ok, found, folio:{folio,fecha,customer_id,email,nombre,reward,points,
//  discount_pct,estado,orden_aplicada}}.
app.get('/rewards/folio', async (req, res) => {
  if (REWARDS_STAFF_PIN && String(req.query.pin || '') !== REWARDS_STAFF_PIN) {
    return res.status(401).json({ ok: false, error: 'PIN de staff invalido' });
  }
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
  if (REWARDS_STAFF_PIN && String(body.pin || '') !== REWARDS_STAFF_PIN) {
    return res.status(401).json({ ok: false, error: 'PIN de staff invalido' });
  }
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
        staff_name: String(body.staff_name || '').trim()
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
      (body.staff_name ? ' por ' + String(body.staff_name).trim() : ''));
    return res.json({ ok: true, updated: true, estado_previo: j.estado_previo || 'pendiente' });
  } catch (e) {
    console.error('[rewards] folio/aplicar error: ' + e.message);
    return res.status(502).json({ ok: false, error: 'error aplicando el folio, intenta de nuevo' });
  }
});


app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v7.2.1 running on port ' + PORT);
  console.log('Whisper transcription: ' + (openai ? 'ENABLED' : 'DISABLED (set OPENAI_API_KEY to enable)'));
  console.log('Auto-summary on conversation opened: ENABLED');
  console.log('Rewards endpoints (/rewards/*): ' + (BOOQABLE_API_KEY ? 'ENABLED' : 'DISABLED (set BOOQABLE_API_KEY)') + (REWARDS_SHEETS_URL ? ', ledger ON' : ', ledger OFF'));
});
