const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { normalizePhone } = require('./_lib/auth');
const crypto = require('crypto');

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

const normalizeAreaCode = (value) => String(value || '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .toUpperCase()
  .replace(/[^A-Z0-9]+/g, '_')
  .replace(/^_+|_+$/g, '');

const buildDeterministicIncidentId = (problemId) => {
  const normalized = firstNonEmpty(problemId).toUpperCase();
  if (!normalized) {
    return null;
  }

  const hash = crypto.createHash('sha1').update(normalized).digest('hex').slice(0, 15);
  let numeric = BigInt(`0x${hash}`);
  if (numeric === 0n) {
    numeric = 1n;
  }

  // Keep generated IDs negative to avoid collisions with regular bigserial growth.
  return `-${numeric.toString()}`;
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
    const causeName = firstNonEmpty(body.cause_name, body.causeName, body.rootCause, body.ProblemRootCause, body.problemRootCause);
    const affectedEntity = firstNonEmpty(body.affected_entity, body.affectedEntity, body.impactedEntity, body.ProblemImpact, body.problemImpact);
    const incomingAreaId = firstNonEmpty(body.area_id, body.areaId);
    const incidentAreaInput = firstNonEmpty(body.incident_area, body.incidentArea, body.area_name, body.areaName, body.area);
    const incidentAreaCodeInput = normalizeAreaCode(firstNonEmpty(body.area_code, body.areaCode, incidentAreaInput));

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
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, created_at')
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
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, created_at')
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

      if (toBoolean(latestIncident.incident_attended, false)) {
        return json(200, {
          reserved: false,
          duplicate: true,
          reason: 'incident_already_acknowledged',
          incident: latestIncident
        });
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
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, created_at')
        .maybeSingle();

      if (reserveError) {
        return json(500, { error: 'Failed to reserve escalation', details: reserveError.message });
      }

      if (!reservedIncident) {
        const { data: refreshedIncident, error: refreshedIncidentError } = await supabase
          .from('incidents')
          .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_id, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, created_at')
          .eq('id', latestIncident.id)
          .maybeSingle();

        if (refreshedIncidentError) {
          return json(500, { error: 'Failed to re-check escalation reservation', details: refreshedIncidentError.message });
        }

        const refreshedIsAcknowledged = toBoolean(refreshedIncident && refreshedIncident.incident_attended, false);
        const refreshedNumberReserved = normalizePhone(refreshedIncident && refreshedIncident.called_number) === calledNumberNormalized;
        let refreshedReason = 'escalation_reservation_conflict';
        if (refreshedIsAcknowledged) {
          refreshedReason = 'incident_already_acknowledged';
        } else if (refreshedNumberReserved) {
          refreshedReason = 'escalation_already_reserved';
        }

        return json(200, {
          reserved: false,
          duplicate: true,
          reason: refreshedReason,
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

    let resolvedArea = null;
    if (incomingAreaId) {
      const { data: areaById, error: areaByIdError } = await supabase
        .from('areas')
        .select('id, code, name, is_active')
        .eq('id', incomingAreaId)
        .maybeSingle();

      if (areaByIdError) {
        return json(500, { error: 'Failed to resolve area by id', details: areaByIdError.message });
      }

      resolvedArea = areaById || null;
    }

    if (!resolvedArea && incidentAreaCodeInput) {
      const { data: areaByCode, error: areaByCodeError } = await supabase
        .from('areas')
        .select('id, code, name, is_active')
        .eq('code', incidentAreaCodeInput)
        .maybeSingle();

      if (areaByCodeError) {
        return json(500, { error: 'Failed to resolve area by code', details: areaByCodeError.message });
      }

      resolvedArea = areaByCode || null;
    }

    if (!resolvedArea && incidentAreaInput) {
      const { data: areaByName, error: areaByNameError } = await supabase
        .from('areas')
        .select('id, code, name, is_active')
        .ilike('name', incidentAreaInput)
        .maybeSingle();

      if (areaByNameError) {
        return json(500, { error: 'Failed to resolve area by name', details: areaByNameError.message });
      }

      resolvedArea = areaByName || null;
    }

    if (resolvedArea && !resolvedArea.is_active) {
      return json(400, { error: 'Resolved area is inactive' });
    }

    const resolvedAreaId = resolvedArea ? resolvedArea.id : null;
    const resolvedIncidentArea = firstNonEmpty(
      resolvedArea && resolvedArea.name,
      incidentAreaInput,
      incidentAreaCodeInput
    );

    let matchedUser = null;
    if (normalized) {
      let userQuery = supabase
        .from('app_users')
        .select('id, username, phone, area_id')
        .eq('phone_normalized', normalized)
        .eq('is_active', true);

      if (resolvedAreaId) {
        userQuery = userQuery.eq('area_id', resolvedAreaId);
      }

      const { data: user, error: userError } = await userQuery.maybeSingle();

      if (userError) {
        return json(500, { error: 'Failed to match user by phone', details: userError.message });
      }

      matchedUser = user || null;
    }

    const deterministicIncidentId = problemId ? buildDeterministicIncidentId(problemId) : null;

    const incidentPayload = {
      ...(deterministicIncidentId ? { id: deterministicIncidentId } : {}),
      problem_id: problemId || null,
      incident_title: incidentTitle,
      incident_status: incidentStatus,
      incident_severity: incidentSeverity,
      incident_description: incidentDescription,
      called_number: calledNumber || null,
      called_user_id: matchedUser ? matchedUser.id : null,
      called_user_name: matchedUser ? matchedUser.username : null,
      ...(resolvedAreaId ? { area_id: resolvedAreaId } : {}),
      ...(resolvedIncidentArea ? { incident_area: resolvedIncidentArea } : {}),
      incident_attended: incidentAttended,
      incident_attended_at: incidentAttended ? new Date().toISOString() : null,
      ...(causeName ? { cause_name: causeName } : {}),
      ...(affectedEntity ? { affected_entity: affectedEntity } : {})
    };

    // Idempotency guard: repeated webhook deliveries for the same problem must not create
    // multiple incident rows, otherwise the call flow can be triggered more than once.
    if (problemId) {
      const { data: existingIncident, error: existingIncidentError } = await supabase
        .from('incidents')
        .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, area_id, incident_area, created_at')
        .eq('problem_id', problemId)
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (existingIncidentError) {
        return json(500, { error: 'Failed to check existing incident', details: existingIncidentError.message });
      }

      if (existingIncident) {
        const duplicatePatch = {};

        if (causeName && !firstNonEmpty(existingIncident.cause_name)) {
          duplicatePatch.cause_name = causeName;
        }

        if (affectedEntity && !firstNonEmpty(existingIncident.affected_entity)) {
          duplicatePatch.affected_entity = affectedEntity;
        }

        if (resolvedAreaId && !firstNonEmpty(existingIncident.area_id)) {
          duplicatePatch.area_id = resolvedAreaId;
        }

        if (resolvedIncidentArea && !firstNonEmpty(existingIncident.incident_area)) {
          duplicatePatch.incident_area = resolvedIncidentArea;
        }

        let mergedIncident = existingIncident;
        if (Object.keys(duplicatePatch).length) {
          const { data: patchedIncident, error: patchedIncidentError } = await supabase
            .from('incidents')
            .update(duplicatePatch)
            .eq('id', existingIncident.id)
            .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, area_id, incident_area, created_at')
            .maybeSingle();

          if (patchedIncidentError) {
            return json(500, { error: 'Failed to enrich duplicate incident', details: patchedIncidentError.message });
          }

          if (patchedIncident) {
            mergedIncident = patchedIncident;
          }
        }

        return json(200, {
          incident: mergedIncident,
          linkedUser: matchedUser
            ? {
                id: matchedUser.id,
                username: matchedUser.username,
                phone: matchedUser.phone
              }
            : null,
          created: false,
          duplicate: true,
          reason: 'problem_id_already_exists',
          updated: Boolean(Object.keys(duplicatePatch).length)
        });
      }
    }

    // Similarity dedupe window suppresses near-identical bursts that may arrive with
    // missing or variant IDs from upstream and would otherwise trigger repeated calls.
    const dedupeWindowMinutes = Math.max(1, parseInt(process.env.INCIDENT_DEDUPE_WINDOW_MINUTES || '15', 10) || 15);
    const strictSimilarityDedupe = toBoolean(process.env.STRICT_SIMILARITY_DEDUPE, true);
    const dedupeSince = new Date(Date.now() - dedupeWindowMinutes * 60 * 1000).toISOString();

    let recentCandidatesQuery = supabase
      .from('incidents')
      .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, area_id, incident_area, created_at')
      .eq('incident_title', incidentTitle)
      .eq('incident_severity', incidentSeverity)
      .gte('created_at', dedupeSince)
      .order('created_at', { ascending: false })
      .limit(25);

    if (resolvedIncidentArea) {
      recentCandidatesQuery = recentCandidatesQuery.eq('incident_area', resolvedIncidentArea);
    }

    const { data: recentCandidates, error: recentCandidatesError } = await recentCandidatesQuery;

    if (recentCandidatesError) {
      return json(500, { error: 'Failed to check recent dedupe candidates', details: recentCandidatesError.message });
    }

    const duplicateCandidate = (Array.isArray(recentCandidates) ? recentCandidates : []).find((item) => {
      const sameNumber = calledNumberNormalized
        ? normalizePhone(item.called_number) === calledNumberNormalized
        : true;

      if (!sameNumber) {
        return false;
      }

      if (strictSimilarityDedupe) {
        return true;
      }

      // If either side has no problem_id, or IDs differ only by source variance,
      // treat close-in-time same-title+severity incidents as duplicates.
      const existingProblemId = firstNonEmpty(item.problem_id).toUpperCase();
      if (!problemId || !existingProblemId) {
        return true;
      }

      return existingProblemId === problemId;
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
        reason: 'recent_similarity_window_match'
      });
    }

    const { data: created, error: incidentError } = await supabase
      .from('incidents')
      .insert(incidentPayload)
      .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, area_id, incident_area, created_at')
      .single();

    if (incidentError) {
      const isDuplicateKey = incidentError.code === '23505' || /duplicate key/i.test(String(incidentError.message || ''));

      if (problemId && isDuplicateKey) {
        const { data: existingAfterConflict, error: conflictLookupError } = await supabase
          .from('incidents')
          .select('id, problem_id, incident_title, incident_status, incident_severity, incident_description, called_number, called_user_name, incident_attended, incident_attended_at, cause_name, affected_entity, area_id, incident_area, created_at')
          .eq('problem_id', problemId)
          .order('created_at', { ascending: false })
          .limit(1)
          .maybeSingle();

        if (conflictLookupError) {
          return json(500, { error: 'Failed to resolve incident conflict', details: conflictLookupError.message });
        }

        if (existingAfterConflict) {
          return json(200, {
            incident: existingAfterConflict,
            linkedUser: matchedUser
              ? {
                  id: matchedUser.id,
                  username: matchedUser.username,
                  phone: matchedUser.phone
                }
              : null,
            created: false,
            duplicate: true,
            reason: 'problem_id_atomic_conflict_duplicate'
          });
        }
      }

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
