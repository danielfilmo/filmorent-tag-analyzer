const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
app.use(express.json());

// Config
const RESPONDIO_API_KEY = process.env.RESPONDIO_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const GOOGLE_SHEETS_URL = process.env.GOOGLE_SHEETS_URL;
const PORT = process.env.PORT || 3000;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

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

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v6' }));

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
 * Does NOT include assignee unless they sent a message.
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
  console.log('\n[' + new Date().toISOString() + '] === WEBHOOK RECEIVED ===');
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

    // Format messages for analysis - include agent name and type
    const formattedMessages = messages.map(msg => {
      const traffic = msg.traffic || msg.type;
      if (traffic === 'incoming') {
        const text = msg.text || msg.message?.text || msg.body || '[media/attachment]';
        return 'CLIENTE: ' + text;
      } else {
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

        const text = msg.text || msg.message?.text || msg.body || '[media/attachment]';
        return label + ': ' + text;
      }
    }).join('\n');

    const channel = messages[0]?.channelType || messages[0]?.channel || 'desconocido';
    const link = 'https://app.respond.io/space/379868/inbox/' + contactId;

    const hasBotAgent = bots.length > 0;
    const hasHumanAgent = humans.length > 0;

    // Build individual agent evaluation instructions
    let humanEvalInstructions = '';
    if (hasHumanAgent) {
      if (humanNames.length === 1) {
        humanEvalInstructions = `
EVALUACION INDIVIDUAL DEL AGENTE HUMANO - Evalua a ${humanNames[0]} basandote SOLO en los mensajes que ESTE agente envio:

"evaluaciones_individuales": [
  {
    "nombre_agente": "${humanNames[0]}",
    "tiempo_respuesta": 8,
    "conocimiento_producto": 7,
    "alternativas_ofrecidas": 9,
    "seguimiento": 6,
    "trato_cliente": 8,
    "cierre_venta": 7,
    "calificacion_general": 7.5,
    "feedback": "Que hizo bien y que puede mejorar este agente, con ejemplos ESPECIFICOS de sus mensajes en la conversacion.",
    "recomendaciones": ["Recomendacion 1", "Recomendacion 2"]
  }
]`;
      } else {
        const agentEntries = humanNames.map(name => `  {
    "nombre_agente": "${name}",
    "tiempo_respuesta": 8,
    "conocimiento_producto": 7,
    "alternativas_ofrecidas": 9,
    "seguimiento": 6,
    "trato_cliente": 8,
    "cierre_venta": 7,
    "calificacion_general": 7.5,
    "feedback": "Que hizo bien y que puede mejorar ESTE agente especificamente, basandote SOLO en SUS mensajes.",
    "recomendaciones": ["Recomendacion especifica para este agente"]
  }`).join(',\n');

        humanEvalInstructions = `
EVALUACION INDIVIDUAL POR AGENTE - IMPORTANTE: Evalua a CADA agente POR SEPARADO basandote UNICAMENTE en los mensajes que ESE agente envio. NO mezcles la evaluacion de un agente con la de otro. Si un agente solo envio 1 mensaje, evalualo solo por ese mensaje.

Criterios por agente:
- tiempo_respuesta: Respondio rapido O tardo mucho despues del mensaje anterior? (10=inmediato, 1=horas/dias sin responder)
- conocimiento_producto: En SUS mensajes, demostro conocer el catalogo y specs? (10=experto, 1=no sabe)
- alternativas_ofrecidas: Ofrecio alternativas cuando algo no estaba disponible? (10=varias opciones, 1=ninguna)
- seguimiento: Hizo follow-up? Cerro la venta? (10=excelente, 1=abandono)
- trato_cliente: Fue amable y profesional en SUS mensajes? (10=excelente, 1=grosero)
- cierre_venta: Contribuyo a cerrar la renta? (10=cerro exitosamente, 5=no aplica, 1=perdio venta)

"evaluaciones_individuales": [
${agentEntries}
]`;
      }
    }

    let botEvalInstructions = '';
    if (hasBotAgent) {
      botEvalInstructions = `
EVALUACION DEL BOT (Filmorent Assistant) - Evalua al agente virtual basandote en SUS mensajes (los marcados con [BOT]):

"evaluacion_bot": {
  "precision_respuestas": 8,
  "manejo_consulta": 7,
  "transicion_humano": 9,
  "tono_comunicacion": 8,
  "calificacion_general": 8,
  "feedback_bot": "Que hizo bien y que deberia mejorar el bot. Se MUY especifico.",
  "mejoras_sugeridas": ["Mejora concreta 1", "Mejora concreta 2"]
}`;
    }

    const analysisPrompt = `Analiza la siguiente conversacion de Filmorent, un negocio de RENTA de equipo de cine y fotografia en Monterrey, Mexico.

TAGS A EVALUAR:
1. "consulta-compra" - El cliente pregunta por COMPRAR equipo (Filmorent solo renta).
2. "equipo-no-disponible" - Equipo no disponible (no existe en catalogo O ya esta rentado).
3. "incidencia" - Problema, queja, equipo dañado, entrega tarde, cobro incorrecto.
4. "renta-perdida" - Cliente queria rentar pero NO se concreto. Causa: "precio", "sin_respuesta_cliente", "tardanza_respuesta", "fechas", "ubicacion", "otro".

AGENTES HUMANOS: ${humanNames.join(', ') || 'Ninguno'}
AGENTE VIRTUAL (BOT): ${botNames.join(', ') || 'Ninguno'}
${humanEvalInstructions}
${botEvalInstructions}

CONVERSACION:
${formattedMessages}

Responde UNICAMENTE con JSON valido (sin markdown, sin backticks, solo JSON puro):
{
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen COMPLETO de 3-5 oraciones: que pidio el cliente, que paso, que alternativas se ofrecieron, y cual fue el RESULTADO FINAL.",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  "evaluaciones_individuales": [${hasHumanAgent ? '...' : ''}],
  "evaluacion_bot": ${hasBotAgent ? '{...}' : 'null'}
}`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
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

    // Log to Google Sheets - v6 format with individual evaluations
    await logToGoogleSheets({
      version: 'v6',
      fecha: new Date().toISOString(),
      contactId: contactId,
      nombre: contactName,
      tags: tagsToApply.join(', '),
      causa_renta_perdida: causaRentaPerdida || '',
      num_mensajes: messages.length,
      canal: channel,
      resumen: resumen,
      link_conversacion: link,
      conversacion_completa: formattedMessages.substring(0, 45000),
      resultado: resultado,
      equipos_solicitados: equipos,
      // v6: Individual evaluations per human agent
      evaluaciones_individuales: evaluacionesIndividuales,
      // v6: Separate bot evaluation
      evaluacion_bot: evaluacionBot,
      // Agent info
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

app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v6 running on port ' + PORT);
});
