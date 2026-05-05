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

const toBoolean = (value, fallback = false) => {
  if (typeof value === 'boolean') {
    return value;
  }

  if (value === null || value === undefined) {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on', 'si', 'sí'].includes(normalized)) {
    return true;
  }

  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }

  return fallback;
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
    const eventType = firstNonEmpty(body.event_type, body.eventType, 'incident_created').toLowerCase();

    const calledNumber = firstNonEmpty(
      body.called_number,
      body.calledNumber,
      body.phone,
      body.to,
      body.notifiedNumber,
      body.notified_number
    );

    const problemId = firstNonEmpty(body.problem_id, body.problemId, body.ProblemID, body.problem_id);
    const incidentAttended = toBoolean(
      body.incident_attended,
      toBoolean(body.incidentAttended, toBoolean(body.attended, false))
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

    if (eventType === 'ack_update') {
      if (!problemId) {
        return json(400, { error: 'problem_id is required for ack_update' });
      }

      let lookupQuery = supabase
        .from('incidents')
        .select('id')
        .eq('problem_id', problemId)
        .order('created_at', { ascending: false })
        .limit(1);

      if (calledNumber) {
        lookupQuery = lookupQuery.eq('called_number', calledNumber);
      }

      const { data: latestIncident, error: lookupError } = await lookupQuery.maybeSingle();
      if (lookupError) {
        return json(500, { error: 'Failed to locate incident for ack update', details: lookupError.message });
      }

      if (!latestIncident) {
        return json(404, { error: 'Incident not found for ack update' });
      }

      const updatePayload = {
        incident_attended: incidentAttended,
        incident_attended_at: incidentAttended ? new Date().toISOString() : null,
        incident_status: incidentStatus
      };

      const { data: updated, error: updateError } = await supabase
        .from('incidents')
        .update(updatePayload)
        .eq('id', latestIncident.id)
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, created_at')
        .single();

      if (updateError) {
        return json(500, { error: 'Failed to update incident acknowledgment', details: updateError.message });
      }

      return json(200, { incident: updated, updated: true });
    }

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
      problem_id: problemId || null,
      incident_title: incidentTitle,
      incident_status: incidentStatus,
      incident_severity: incidentSeverity,
      incident_description: incidentDescription,
      called_number: calledNumber || null,
      called_user_id: matchedUser ? matchedUser.id : null,
      called_user_name: matchedUser ? matchedUser.username : null,
      incident_attended: incidentAttended,
      incident_attended_at: incidentAttended ? new Date().toISOString() : null
    };

    const { data: created, error: incidentError } = await supabase
      .from('incidents')
      .insert(incidentPayload)
      .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, created_at')
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
