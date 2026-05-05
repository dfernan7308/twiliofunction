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

    const problemId = firstNonEmpty(body.problem_id, body.problemId, body.ProblemID, body.problem_id).toUpperCase();
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
    const calledNumberNormalized = normalized;
    const supabase = getSupabaseAdmin();

    if (eventType === 'ack_update') {
      if (!problemId && !calledNumberNormalized) {
        return json(400, { error: 'problem_id or called_number is required for ack_update' });
      }

      let latestIncident = null;

      if (problemId) {
        const { data: byProblem, error: byProblemError } = await supabase
          .from('incidents')
          .select('id, called_number')
          .eq('problem_id', problemId)
          .order('created_at', { ascending: false })
          .limit(10);

        if (byProblemError) {
          return json(500, { error: 'Failed to locate incident for ack update', details: byProblemError.message });
        }

        if (Array.isArray(byProblem) && byProblem.length) {
          if (calledNumberNormalized) {
            latestIncident = byProblem.find((item) => normalizePhone(item.called_number) === calledNumberNormalized) || byProblem[0];
          } else {
            latestIncident = byProblem[0];
          }
        }
      }

      if (!latestIncident && calledNumberNormalized) {
        const { data: recentCandidates, error: recentError } = await supabase
          .from('incidents')
          .select('id, called_number')
          .order('created_at', { ascending: false })
          .limit(40);

        if (recentError) {
          return json(500, { error: 'Failed to locate incident by called number', details: recentError.message });
        }

        if (Array.isArray(recentCandidates) && recentCandidates.length) {
          latestIncident = recentCandidates.find((item) => normalizePhone(item.called_number) === calledNumberNormalized) || null;
        }
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

    if (eventType === 'escalation_reserve') {
      if (!problemId || !calledNumber) {
        return json(400, { error: 'problem_id and called_number are required for escalation_reserve' });
      }

      const reserveLevel = firstNonEmpty(body.level, '2');
      const reserveStatus = firstNonEmpty(body.incident_status, body.reserve_status, `ESCALATION_RESERVED_L${reserveLevel}`);

      let reserveUser = null;
      if (normalized) {
        const { data: reserveMatchedUser, error: reserveUserError } = await supabase
          .from('app_users')
          .select('id, username, phone')
          .eq('phone_normalized', normalized)
          .eq('is_active', true)
          .maybeSingle();

        if (reserveUserError) {
          return json(500, { error: 'Failed to match escalation user by phone', details: reserveUserError.message });
        }

        reserveUser = reserveMatchedUser || null;
      }

      const { data: latestIncident, error: latestIncidentError } = await supabase
        .from('incidents')
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, created_at')
        .eq('problem_id', problemId)
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (latestIncidentError) {
        return json(500, { error: 'Failed to locate incident for escalation reserve', details: latestIncidentError.message });
      }

      if (!latestIncident) {
        return json(404, { error: 'Incident not found for escalation reserve' });
      }

      if (normalizePhone(latestIncident.called_number) === calledNumberNormalized) {
        return json(200, {
          reserved: false,
          duplicate: true,
          reason: 'escalation_already_reserved',
          incident: latestIncident
        });
      }

      const currentStatus = firstNonEmpty(latestIncident.incident_status, 'OPEN');
      const { data: reservedIncident, error: reserveError } = await supabase
        .from('incidents')
        .update({
          called_number: calledNumber,
          called_user_id: reserveUser ? reserveUser.id : latestIncident.called_user_id,
          called_user_name: reserveUser ? reserveUser.username : latestIncident.called_user_name,
          incident_status: reserveStatus
        })
        .eq('id', latestIncident.id)
        .eq('incident_status', currentStatus)
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, created_at')
        .maybeSingle();

      if (reserveError) {
        return json(500, { error: 'Failed to reserve escalation', details: reserveError.message });
      }

      if (!reservedIncident) {
        const { data: refreshedIncident, error: refreshedIncidentError } = await supabase
          .from('incidents')
          .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, created_at')
          .eq('id', latestIncident.id)
          .maybeSingle();

        if (refreshedIncidentError) {
          return json(500, { error: 'Failed to re-check escalation reservation', details: refreshedIncidentError.message });
        }

        return json(200, {
          reserved: false,
          duplicate: true,
          reason: 'escalation_reservation_conflict',
          incident: refreshedIncident || latestIncident
        });
      }

      return json(200, {
        reserved: true,
        duplicate: false,
        incident: reservedIncident,
        linkedUser: reserveUser
          ? {
              id: reserveUser.id,
              username: reserveUser.username,
              phone: reserveUser.phone
            }
          : null
      });
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

    // Idempotency guard: repeated webhook deliveries for the same problem must not create
    // multiple incident rows, otherwise the call flow can be triggered more than once.
    if (problemId) {
      const { data: existingIncident, error: existingIncidentError } = await supabase
        .from('incidents')
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, created_at')
        .eq('problem_id', problemId)
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (existingIncidentError) {
        return json(500, { error: 'Failed to check existing incident', details: existingIncidentError.message });
      }

      if (existingIncident) {
        return json(200, {
          incident: existingIncident,
          linkedUser: matchedUser
            ? {
                id: matchedUser.id,
                username: matchedUser.username,
                phone: matchedUser.phone
              }
            : null,
          created: false,
          duplicate: true,
          reason: 'problem_id_already_exists'
        });
      }
    }

    // Fallback dedupe when source does not provide problem_id.
    // This suppresses near-identical incident bursts that can trigger repeated calls.
    if (!problemId) {
      const dedupeWindowMinutes = Math.max(1, parseInt(process.env.INCIDENT_DEDUPE_WINDOW_MINUTES || '15', 10) || 15);
      const dedupeSince = new Date(Date.now() - dedupeWindowMinutes * 60 * 1000).toISOString();

      const { data: recentCandidates, error: recentCandidatesError } = await supabase
        .from('incidents')
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, created_at')
        .eq('incident_title', incidentTitle)
        .eq('incident_severity', incidentSeverity)
        .gte('created_at', dedupeSince)
        .order('created_at', { ascending: false })
        .limit(25);

      if (recentCandidatesError) {
        return json(500, { error: 'Failed to check fallback dedupe candidates', details: recentCandidatesError.message });
      }

      const duplicateCandidate = (Array.isArray(recentCandidates) ? recentCandidates : []).find((item) => {
        if (!calledNumberNormalized) {
          return true;
        }

        return normalizePhone(item.called_number) === calledNumberNormalized;
      });

      if (duplicateCandidate) {
        return json(200, {
          incident: duplicateCandidate,
          linkedUser: matchedUser
            ? {
                id: matchedUser.id,
                username: matchedUser.username,
                phone: matchedUser.phone
              }
            : null,
          created: false,
          duplicate: true,
          reason: 'fallback_dedupe_window_match'
        });
      }
    }

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
        : null,
      created: true,
      duplicate: false
    });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
