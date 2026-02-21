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

// Agent mapping: userId -> {name, email}
// Auto-updated from assignee data on each conversation
const AGENT_MAP = {
  1026911: { name: 'Daniel Alonso', email: 'daniel@filmorent.com' },
  1027747: { name: 'Barush Villarreal', email: 'barush@filmorent.com' },
  1027751: { name: 'Alfredo Celedon', email: 'alfredo@filmorent.com' },
  1027755: { name: 'Eddy Manzano', email: 'eddy@filmorent.com' },
  1027757: { name: 'Diego Tovar', email: 'diego@filmorent.com' },
  1027820: { name: 'Suheidi Dominguez', email: 'administracion@filmorent.com' }
};

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', version: 'v4' }));

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
 * Extract all unique agents from outgoing messages + contact assignee.
 * Returns array of {userId, name, email}
 */
function extractAgentsFromMessages(messages, assignee) {
  const agentMap = new Map();

  // First, add assignee if available (this gives us name+email for sure)
  if (assignee && assignee.id) {
    const name = ((assignee.firstName || '') + ' ' + (assignee.lastName || '')).trim();
    agentMap.set(assignee.id, {
      userId: assignee.id,
      name: name,
      email: assignee.email || ''
    });
    // Update global mapping
    if (!AGENT_MAP[assignee.id]) {
      AGENT_MAP[assignee.id] = { name, email: assignee.email || '' };
      console.log('New agent discovered: ' + name + ' (ID: ' + assignee.id + ')');
    }
  }

  // Then, scan all outgoing messages for unique sender userIds
  for (const msg of messages) {
    const traffic = msg.traffic || msg.type;
    if (traffic === 'outgoing' && msg.sender && msg.sender.userId) {
      const uid = msg.sender.userId;
      if (!agentMap.has(uid)) {
        // Look up in global mapping
        const known = AGENT_MAP[uid];
        agentMap.set(uid, {
          userId: uid,
          name: known ? known.name : 'Agente #' + uid,
          email: known ? known.email : ''
        });
      }
    }
  }

  return Array.from(agentMap.values());
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

    // Get assignee from contact data
    let assignee = null;
    if (contactResponse.ok) {
      const contactData = await contactResponse.json();
      assignee = contactData.assignee || null;
    }

    // Extract all agents who participated
    const agents = extractAgentsFromMessages(messages, assignee);
    const agentNames = agents.map(a => a.name).join(', ');
    const agentEmails = agents.map(a => a.email).filter(e => e).join(', ');
    console.log('Agents detected: ' + agentNames);

    // Format messages for analysis - now include agent name when possible
    const formattedMessages = messages.map(msg => {
      const traffic = msg.traffic || msg.type;
      if (traffic === 'incoming') {
        const text = msg.text || msg.message?.text || msg.body || '[media/attachment]';
        return 'CLIENTE: ' + text;
      } else {
        const uid = msg.sender?.userId;
        const agentInfo = uid && AGENT_MAP[uid] ? AGENT_MAP[uid].name : 'AGENTE';
        const text = msg.text || msg.message?.text || msg.body || '[media/attachment]';
        return agentInfo + ': ' + text;
      }
    }).join('\n');

    const channel = messages[0]?.channelType || messages[0]?.channel || 'desconocido';
    const link = 'https://app.respond.io/space/379868/inbox/' + contactId;

    const analysisPrompt = `Analiza la siguiente conversacion de Filmorent, un negocio de RENTA de equipo de cine y fotografia en Monterrey, Mexico.

TAGS A EVALUAR:
1. "consulta-compra" - El cliente pregunta por COMPRAR equipo (Filmorent solo renta).
2. "equipo-no-disponible" - Equipo no disponible (no existe en catalogo O ya esta rentado).
3. "incidencia" - Problema, queja, equipo dañado, entrega tarde, cobro incorrecto.
4. "renta-perdida" - Cliente queria rentar pero NO se concreto. Causa: "precio", "sin_respuesta_cliente", "tardanza_respuesta", "fechas", "ubicacion", "otro".

AGENTES QUE PARTICIPARON: ${agentNames}

EVALUACION DEL AGENTE - Califica de 1 a 10 en cada aspecto. Si hubo multiples agentes, evalua al EQUIPO en general pero menciona a cada agente por nombre en el feedback:
- tiempo_respuesta: Respondio rapido o tardo mucho? (10=inmediato, 1=horas/dias sin responder)
- conocimiento_producto: Conoce bien el catalogo y specs del equipo? (10=experto, 1=no sabe)
- alternativas_ofrecidas: Ofrecio alternativas cuando algo no estaba disponible? (10=multiples opciones relevantes, 1=no ofrecio nada)
- seguimiento: Hizo follow-up si el cliente no contesto? Cerro la venta? (10=excelente seguimiento, 1=abandono la conversacion)
- trato_cliente: Fue amable, profesional, resolvio todas las dudas? (10=excelente, 1=grosero/negligente)
- cierre_venta: Logro concretar la renta? Proceso el pedido correctamente? (10=cerro exitosamente, 5=no aplica, 1=perdio venta evitable)

CONVERSACION:
${formattedMessages}

Responde UNICAMENTE con JSON valido (sin markdown, sin backticks, solo JSON puro):
{
  "tags": ["tag1"],
  "causa_renta_perdida": "causa o null",
  "resumen": "Resumen COMPLETO de 3-5 oraciones: que pidio el cliente, que paso durante la conversacion, que alternativas se ofrecieron, y cual fue el RESULTADO FINAL (se concreto la renta? el cliente dejo de contestar? se fue a otro lado? quedo pendiente?)",
  "resultado": "concretada | perdida | pendiente | no_aplica",
  "equipos_solicitados": [{"nombre": "equipo", "disponible": true}],
  "evaluacion_agente": {
    "tiempo_respuesta": 8,
    "conocimiento_producto": 7,
    "alternativas_ofrecidas": 9,
    "seguimiento": 6,
    "trato_cliente": 8,
    "cierre_venta": 7,
    "calificacion_general": 7.5,
    "feedback": "Que hizo bien y que puede mejorar cada agente, siendo especifico con ejemplos de la conversacion. Menciona a cada agente por nombre.",
    "recomendaciones": ["Recomendacion especifica 1", "Recomendacion especifica 2"]
  }
}`;

    const claudeResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      messages: [{ role: 'user', content: analysisPrompt }]
    });

    const analysisText = claudeResponse.content[0].text.trim();
    console.log('Claude analysis: ' + analysisText);

    let tagsToApply = [];
    let causaRentaPerdida = null;
    let resumen = '';
    let equipos = [];
    let evaluacion = null;
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
      evaluacion = parsed.evaluacion_agente || null;
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

    // Log to Google Sheets with agent data
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
      evaluacion_agente: evaluacion,
      agentes: agents.map(a => ({ nombre: a.name, email: a.email, userId: a.userId }))
    });

    const calif = evaluacion ? evaluacion.calificacion_general : 'N/A';
    console.log('=== DONE: contact=' + contactId + ', agents=' + agentNames + ', calif=' + calif + '/10 ===\n');

  } catch (error) {
    console.error('Error:', error.message);
  }
});

app.listen(PORT, () => {
  console.log('Filmorent Tag Analyzer v4 running on port ' + PORT);
});
