const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { normalizePhone } = require('./_lib/auth');

const firstNonEmpty = (...values) => {
  for (const value of values) {
    if (value !== null && value !== undefined && String(value).trim() !== '') {
      return String(value).trim();
    }
  }
  return '';
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'POST') {
    return json(405, { error: 'Method not allowed' });
  }

  const expectedSecret = process.env.WEBHOOK_SHARED_SECRET || '';
  if (expectedSecret) {
    const incomingSecret = (event.headers && (event.headers['x-webhook-secret'] || event.headers['X-Webhook-Secret'])) || '';
    if (incomingSecret !== expectedSecret) {
      return json(401, { error: 'Invalid webhook secret' });
    }
  }

  try {
    const body = parseJsonBody(event);

    const calledNumber = firstNonEmpty(
      body.called_number,
      body.calledNumber,
      body.phone,
      body.to,
      body.notifiedNumber,
      body.notified_number
    );

    const incidentTitle = firstNonEmpty(
      body.incident_title,
      body.title,
      body.problemTitle,
      body.eventName,
      'Incidente sin titulo'
    );

    const incidentSeverity = firstNonEmpty(body.incident_severity, body.severity, body.priority, 'UNKNOWN');
    const incidentStatus = firstNonEmpty(body.incident_status, body.status, 'OPEN');
    const incidentDescription = firstNonEmpty(body.incident_description, body.description, body.details, body.message);

    const normalized = normalizePhone(calledNumber);
    const supabase = getSupabaseAdmin();

    let matchedUser = null;
    if (normalized) {
      const { data: user, error: userError } = await supabase
        .from('app_users')
        .select('id, username, phone')
        .eq('phone_normalized', normalized)
        .eq('is_active', true)
        .maybeSingle();

      if (userError) {
        return json(500, { error: 'Failed to match user by phone', details: userError.message });
      }

      matchedUser = user || null;
    }

    const incidentPayload = {
      incident_title: incidentTitle,
      incident_status: incidentStatus,
      incident_severity: incidentSeverity,
      incident_description: incidentDescription,
      called_number: calledNumber || null,
      called_user_id: matchedUser ? matchedUser.id : null,
      called_user_name: matchedUser ? matchedUser.username : null
    };

    const { data: created, error: incidentError } = await supabase
      .from('incidents')
      .insert(incidentPayload)
      .select('id, incident_title, called_number, called_user_name, created_at')
      .single();

    if (incidentError) {
      return json(500, { error: 'Failed to create incident', details: incidentError.message });
    }

    return json(201, {
      incident: created,
      linkedUser: matchedUser
        ? {
            id: matchedUser.id,
            username: matchedUser.username,
            phone: matchedUser.phone
          }
        : null
    });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
