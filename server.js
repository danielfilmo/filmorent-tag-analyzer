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
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v7.2.1', whisper: !!openai, autoSummary: true }));

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

    const analysisText = claudeResponse.content[0].text.trim();
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

    const summary = claudeResponse.content[0].text.trim();
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

    const analysisText = claudeResponse.content[0].text.trim();
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
        log.push({ field: field, fmt: fmt, status: r.status, items: r.items.length });
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


app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v7.2.1 running on port ' + PORT);
  console.log('Whisper transcription: ' + (openai ? 'ENABLED' : 'DISABLED (set OPENAI_API_KEY to enable)'));
  console.log('Auto-summary on conversation opened: ENABLED');
});
