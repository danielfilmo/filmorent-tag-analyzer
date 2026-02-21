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
// type: "human" for team members, "bot" for AI agents
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
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v5' }));

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
 * Extract agents who ACTUALLY SENT messages in the conversation.
 * Uses sender.source to distinguish:
 *   - "user" = human agent (with userId identifying who)
 *   - "ai_agent" = bot/virtual agent
 *
 * IMPORTANT: Does NOT include assignee unless they actually sent a message.
 * This fixes the bug where the assigned agent was listed even if they never responded.
 *
 * Returns: { humans: [{userId, name, email}], bots: [{userId, name}], all: [{userId, name, email, type}] }
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
      // Bot/AI agent
      if (!botAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        botAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente Virtual #' + uid,
          email: '',
          type: 'bot'
        });
        // Auto-discover new bots
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente Virtual #' + uid, email: '', type: 'bot' };
          console.log('New bot discovered: ID ' + uid);
        }
      }
    } else if (source === 'user' && uid) {
      // Human agent
      if (!humanAgents.has(uid)) {
        const known = AGENT_MAP[uid];
        humanAgents.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente #' + uid,
          email: known ? known.email : '',
          type: 'human'
        });
        // Auto-discover new agents
        if (!AGENT_MAP[uid]) {
          AGENT_MAP[uid] = { name: 'Agente #' + uid, email: '', type: 'human' };
          console.log('New agent discovered: ID ' + uid);
        }
      }
    } else if (source === 'workflow') {
      // Workflow/automation message - treat as bot
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

  const humans = Array.from(humanAgents.values());
  const bots = Array.from(botAgents.values());
  const all = [...bots, ...humans]; // Bots first (usually respond first), then humans

  return { humans, bots, all };
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

    // Get assignee from contact data (for logging only, NOT for agent detection)
    let assignee = null;
    if (contactResponse.ok) {
      const contactData = await contactResponse.json();
      assignee = contactData.assignee || null;
    }

    // Extract agents who ACTUALLY sent messages (not just assigned)
    const { humans, bots, all: allAgents } = extractAgentsFromMessages(messages);
    const humanNames = humans.map(a => a.name).join(', ') || 'Ninguno';
    const botNames = bots.map(a => a.name).join(', ') || 'Ninguno';
    const allNames = allAgents.map(a => a.name).join(', ') || 'Sin agente';
    const agentEmails = humans.map(a => a.email).filter(e => e).join(', ');

    console.log('Agents detected - Humans: ' + humanNames + ' | Bots: ' + botNames);
    if (assignee) {
      console.log('Assignee (for reference): ' + assignee.firstName + ' ' + (assignee.lastName || ''));
    }

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

    // Build the evaluation section based on whether there's a bot
    const hasBotAgent = bots.length > 0;
    const hasHumanAgent = humans.length > 0;

    let evaluationInstructions = '';
    if (hasBotAgent && hasHumanAgent) {
      evaluationInstructions = `
EVALUACION - Hay AGENTE VIRTUAL (bot) y AGENTES HUMANOS en esta conversacion. Evalua AMBOS por separado:

evaluacion_bot (del Filmorent Assistant / agente virtual):
- precision_respuestas: Las respuestas del bot fueron correctas y relevantes? (10=perfecto, 1=informacion erronea)
- manejo_consulta: Entendio bien lo que el cliente necesitaba? (10=perfecto entendimiento, 1=no entendio nada)
- transicion_humano: Paso al agente humano en el momento correcto? (10=transicion perfecta, 1=debio pasar antes/despues)
- tono_comunicacion: El tono fue natural, amable y profesional? (10=excelente, 1=robotico/frio)
- feedback_bot: Que hizo bien y que deberia mejorar el bot. Se MUY especifico con ejemplos de la conversacion.
- mejoras_sugeridas: ["Sugerencia concreta 1 para mejorar el bot", "Sugerencia 2"]

evaluacion_agente (de los agentes humanos: ${humanNames}):
- tiempo_respuesta: Respondio rapido o tardo mucho? (10=inmediato, 1=horas/dias sin responder)
- conocimiento_producto: Conoce bien el catalogo y specs del equipo? (10=experto, 1=no sabe)
- alternativas_ofrecidas: Ofrecio alternativas cuando algo no estaba disponible? (10=multiples opciones relevantes, 1=no ofrecio nada)
- seguimiento: Hizo follow-up si el cliente no contesto? Cerro la venta? (10=excelente seguimiento, 1=abandono la conversacion)
- trato_cliente: Fue amable, profesional, resolvio todas las dudas? (10=excelente, 1=grosero/negligente)
- cierre_venta: Logro concretar la renta? Proceso el pedido correctamente? (10=cerro exitosamente, 5=no aplica, 1=perdio venta evitable)`;
    } else if (hasBotAgent) {
      evaluationInstructions = `
EVALUACION - SOLO el agente virtual (bot) respondio en esta conversacion:

evaluacion_bot (del Filmorent Assistant):
- precision_respuestas: Las respuestas del bot fueron correctas y relevantes? (10=perfecto, 1=informacion erronea)
- manejo_consulta: Entendio bien lo que el cliente necesitaba? (10=perfecto entendimiento, 1=no entendio nada)
- transicion_humano: Paso al agente humano en el momento correcto? (10=transicion perfecta, 1=debio pasar antes/despues, 5=no fue necesario)
- tono_comunicacion: El tono fue natural, amable y profesional? (10=excelente, 1=robotico/frio)
- feedback_bot: Que hizo bien y que deberia mejorar el bot.
- mejoras_sugeridas: ["Sugerencia concreta 1", "Sugerencia 2"]

evaluacion_agente: null (no hubo agente humano)`;
    } else {
      evaluationInstructions = `
EVALUACION DEL AGENTE HUMANO (${humanNames}):
- tiempo_respuesta: Respondio rapido o tardo mucho? (10=inmediato, 1=horas/dias sin responder)
- conocimiento_producto: Conoce bien el catalogo y specs del equipo? (10=experto, 1=no sabe)
- alternativas_ofrecidas: Ofrecio alternativas cuando algo no estaba disponible? (10=multiples opciones relevantes, 1=no ofrecio nada)
- seguimiento: Hizo follow-up si el cliente no contesto? Cerro la venta? (10=excelente seguimiento, 1=abandono la conversacion)
- trato_cliente: Fue amable, profesional, resolvio todas las dudas? (10=excelente, 1=grosero/negligente)
- cierre_venta: Logro concretar la renta? Proceso el pedido correctamente? (10=cerro exitosamente, 5=no aplica, 1=perdio venta evitable)`;
    }

    // Build expected JSON structure description
    let jsonStructure = '';
    if (hasBotAgent && hasHumanAgent) {
      jsonStructure = `{
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen COMPLETO de 3-5 oraciones",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  "evaluacion_bot": {
    "precision_respuestas": 8,
    "manejo_consulta": 7,
    "transicion_humano": 9,
    "tono_comunicacion": 8,
    "calificacion_general": 8,
    "feedback_bot": "Feedback especifico del bot",
    "mejoras_sugeridas": ["Mejora 1", "Mejora 2"]
  },
  "evaluacion_agente": {
    "tiempo_respuesta": 8,
    "conocimiento_producto": 7,
    "alternativas_ofrecidas": 9,
    "seguimiento": 6,
    "trato_cliente": 8,
    "cierre_venta": 7,
    "calificacion_general": 7.5,
    "feedback": "Feedback especifico por agente",
    "recomendaciones": ["Recomendacion 1", "Recomendacion 2"]
  }
}`;
    } else if (hasBotAgent) {
      jsonStructure = `{
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen COMPLETO de 3-5 oraciones",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  "evaluacion_bot": {
    "precision_respuestas": 8,
    "manejo_consulta": 7,
    "transicion_humano": 9,
    "tono_comunicacion": 8,
    "calificacion_general": 8,
    "feedback_bot": "Feedback especifico del bot",
    "mejoras_sugeridas": ["Mejora 1", "Mejora 2"]
  },
  "evaluacion_agente": null
}`;
    } else {
      jsonStructure = `{
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen COMPLETO de 3-5 oraciones",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  "evaluacion_bot": null,
  "evaluacion_agente": {
    "tiempo_respuesta": 8,
    "conocimiento_producto": 7,
    "alternativas_ofrecidas": 9,
    "seguimiento": 6,
    "trato_cliente": 8,
    "cierre_venta": 7,
    "calificacion_general": 7.5,
    "feedback": "Feedback especifico por agente",
    "recomendaciones": ["Recomendacion 1", "Recomendacion 2"]
  }
}`;
    }

    const analysisPrompt = `Analiza la siguiente conversacion de Filmorent, un negocio de RENTA de equipo de cine y fotografia en Monterrey, Mexico.

TAGS A EVALUAR:
1. "consulta-compra" - El cliente pregunta por COMPRAR equipo (Filmorent solo renta).
2. "equipo-no-disponible" - Equipo no disponible (no existe en catalogo O ya esta rentado).
3. "incidencia" - Problema, queja, equipo dañado, entrega tarde, cobro incorrecto.
4. "renta-perdida" - Cliente queria rentar pero NO se concreto. Causa: "precio", "sin_respuesta_cliente", "tardanza_respuesta", "fechas", "ubicacion", "otro".

AGENTES HUMANOS QUE RESPONDIERON: ${humanNames}
AGENTE VIRTUAL (BOT): ${botNames}
${evaluationInstructions}

CONVERSACION:
${formattedMessages}

Responde UNICAMENTE con JSON valido (sin markdown, sin backticks, solo JSON puro):
${jsonStructure}`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1500,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeResponse.content[0].text.trim();
    console.log('Claude analysis: ' + analysisText);

    let tagsToApply = [];
    let causaRentaPerdida = null;
    let resumen = '';
    let equipos = [];
    let evaluacionAgente = null;
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
      evaluacionAgente = parsed.evaluacion_agente || null;
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
      const causeTag = 'renta-perdida:' + causaRentaPerdida;
      tagsToApply.push(causeTag);
      console.log('Renta perdida causa: ' + causaRentaPerdida);
    }

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
        console.log('Tags applied successfully: ' + tagsToApply.join(', '));
      }
    } else {
      console.log('No tags to apply');
    }

    // Log to Google Sheets with agent data (v5: separate human and bot data)
    await logToGoogleSheets({
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
      evaluacion_agente: evaluacionAgente,
      evaluacion_bot: evaluacionBot,
      agentes_humanos: humans.map(a => ({ nombre: a.name, email: a.email, userId: a.userId })),
      agentes_bot: bots.map(a => ({ nombre: a.name, userId: a.userId })),
      agentes_todos: allNames,
      assignee: assignee ? (assignee.firstName + ' ' + (assignee.lastName || '')).trim() : 'Sin asignar'
    });

    const califHumano = evaluacionAgente ? evaluacionAgente.calificacion_general : 'N/A';
    const califBot = evaluacionBot ? evaluacionBot.calificacion_general : 'N/A';
    console.log('=== DONE: contact=' + contactId + ', humans=' + humanNames + ', bot=' + botNames + ', calif_humano=' + califHumano + '/10, calif_bot=' + califBot + '/10 ===\n');

  } catch (error) {
    console.error('Error:', error.message);
  }
});

app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v5 running on port ' + PORT);
});
