const xmlEscape = (value) => String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');

const crypto = require('crypto');

const FLOW_LOCKS = global.__twilioFlowLocks || (global.__twilioFlowLocks = new Map());

const cleanupExpiredFlowLocks = () => {
    const now = Date.now();
    for (const [storedKey, expiresAt] of FLOW_LOCKS.entries()) {
        if (Number(expiresAt) <= now) {
            FLOW_LOCKS.delete(storedKey);
        }
    }
};

const setFlowLock = (key, ttlMs) => {
    cleanupExpiredFlowLocks();
    FLOW_LOCKS.set(key, Date.now() + Math.max(1000, Number(ttlMs) || 1000));
};

const hasActiveFlowLock = (key) => {
    cleanupExpiredFlowLocks();
    return Number(FLOW_LOCKS.get(key) || 0) > Date.now();
};

const asBooleanFlag = (value, fallback = false) => {
    if (value === null || value === undefined) {
        return fallback;
    }

    return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
};

const acquireFlowLock = (key, ttlMs) => {
    const now = Date.now();
    cleanupExpiredFlowLocks();

    const existingExpiresAt = Number(FLOW_LOCKS.get(key) || 0);
    if (existingExpiresAt > now) {
        return false;
    }

    setFlowLock(key, ttlMs);
    return true;
};

const isSyntheticProblemId = (value) => String(value || '').startsWith('fp-');

const cleanForSpeech = (value) => String(value || '')
    .replace(/[\r\n]+/g, '. ')
    .replace(/[_/]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const truncateText = (value, maxLength) => {
    const text = cleanForSpeech(value);
    if (!maxLength || text.length <= maxLength) {
        return text;
    }

    return `${text.slice(0, Math.max(0, maxLength - 3)).trim()}...`;
};

const getCompactDetails = (value, maxLength = 160) => {
    const text = cleanForSpeech(value);
    if (!text) {
        return '';
    }

    const firstSentence = text.split(/(?<=[.!?])\s+/)[0] || text;
    return truncateText(firstSentence, maxLength);
};

const ensureSentence = (value) => {
    const text = cleanForSpeech(value);
    if (!text) {
        return '';
    }

    return /[.!?]$/.test(text) ? text : `${text}.`;
};

const toSpanishDescription = (value) => {
    let text = cleanForSpeech(value);
    if (!text) {
        return '';
    }

    const replacements = [
        [/The error rate increased to\s+([^.]+?)\./gi, 'La tasa de error aumentó a $1.'],
        [/Endpoint\s+([^.]+?)\s+has a failure rate increase\./gi, 'El endpoint $1 presenta un aumento en la tasa de error.'],
        [/Service\s+([^.]+?)\s+has a failure rate increase\./gi, 'El servicio $1 presenta un aumento en la tasa de error.'],
        [/The current response time\s*\(([^)]+)\)\s*exceeds the auto-detected baseline\s*\(([^)]+)\)\s*by\s*([^.]+?)\./gi, 'El tiempo de respuesta actual ($1) supera la línea base detectada automáticamente ($2) en $3.'],
        [/Endpoint\s+([^.]+?)\s+has a slowdown\./gi, 'El endpoint $1 presenta degradación en el tiempo de respuesta.'],
        [/Service\s+([^.]+?)\s+has a slowdown\./gi, 'El servicio $1 presenta degradación en el tiempo de respuesta.'],
        [/The html element could not be found to perform action from\s+([^.]+?)\.?$/gi, 'No se encontró el elemento HTML necesario para ejecutar la acción desde $1.'],
        [/OPEN Problem\s+([^.]+?)\.?$/gi, 'Problema abierto $1.'],
        [/CLOSED Problem\s+([^.]+?)\.?$/gi, 'Problema cerrado $1.'],
        [/Multiple service problems/gi, 'Múltiples problemas de servicio'],
        [/Failure rate increase/gi, 'Aumento en la tasa de error'],
        [/Response time degradation/gi, 'Degradación en el tiempo de respuesta'],
        [/Browser monitor global outage/gi, 'Caída global del monitor de navegador']
    ];

    replacements.forEach(([pattern, replacement]) => {
        text = text.replace(pattern, replacement);
    });

    return ensureSentence(text);
};

const buildDescriptiveDetails = ({ title, causeName, affectedEntity, details, status, affectedUrl }) => {
    const translatedDetails = toSpanishDescription(details);
    const sentences = translatedDetails
        ? translatedDetails.split(/(?<=[.!?])\s+/).filter(Boolean).slice(0, 2)
        : [];

    const narrativeParts = [];

    if (sentences.length) {
        narrativeParts.push(sentences.join(' '));
    } else if (title) {
        narrativeParts.push(`Se detectó el incidente ${toSpanishDescription(title)}`);
    }

    if (causeName && !narrativeParts.join(' ').toLowerCase().includes(cleanForSpeech(causeName).toLowerCase())) {
        narrativeParts.push(`Causa identificada: ${cleanForSpeech(causeName)}.`);
    }

    if (affectedEntity && !narrativeParts.join(' ').toLowerCase().includes(cleanForSpeech(affectedEntity).toLowerCase())) {
        narrativeParts.push(`Entidad afectada: ${cleanForSpeech(affectedEntity)}.`);
    }

    if (status && !narrativeParts.join(' ').toLowerCase().includes(cleanForSpeech(status).toLowerCase())) {
        narrativeParts.push(`Estado actual: ${cleanForSpeech(status)}.`);
    }

    if (affectedUrl) {
        narrativeParts.push(`URL afectada: ${cleanForSpeech(affectedUrl)}.`);
    }

    return truncateText(narrativeParts.map((part) => ensureSentence(part)).filter(Boolean).join(' '), 360);
};

const normalizeList = (value) => {
    if (!value) {
        return [];
    }

    if (Array.isArray(value)) {
        return value.filter(Boolean).map((item) => String(item).trim()).filter(Boolean);
    }

    return String(value)
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
};

const buildTeamsWebhookUrl = (context, event) => {
    const directUrl = context.TEAMS_WEBHOOK_URL || event.teamsWebhookUrl;
    if (directUrl) {
        return String(directUrl).trim();
    }

    const csvParts = String(context.TEAMS_WEBHOOK_URL_PARTS || event.teamsWebhookUrlParts || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);

    if (csvParts.length) {
        return csvParts.join('');
    }

    const parts = [
        context.TEAMS_WEBHOOK_URL_PART1 || event.teamsWebhookUrlPart1,
        context.TEAMS_WEBHOOK_URL_PART2 || event.teamsWebhookUrlPart2,
        context.TEAMS_WEBHOOK_URL_PART3 || event.teamsWebhookUrlPart3,
        context.TEAMS_WEBHOOK_URL_PART4 || event.teamsWebhookUrlPart4
    ].filter(Boolean);

    return parts.length ? parts.join('') : '';
};

const isDebugEnabled = (value) => ['1', 'true', 'yes', 'on'].includes(String(value || '').toLowerCase());

const shouldEnrichFromDynatrace = (context, payload) => {
    const flag = firstNonEmpty(context.DT_ENRICH_PROBLEMS, payload.dtEnrichProblems, 'true').toLowerCase();
    return ['1', 'true', 'yes', 'on'].includes(flag);
};

const hasDynatraceIdentitySignals = (payload) => Boolean(firstNonEmpty(
    payload.problemUrl,
    payload.ProblemURL,
    payload.url,
    payload.problemURL,
    payload.displayId,
    payload.DisplayID,
    payload.display_id,
    payload.problemDisplayId,
    payload['event.id'],
    payload['event.problemId'],
    payload['event.problem_id'],
    payload.event && payload.event.id,
    payload.event && payload.event.problemId,
    payload.event && payload.event.problem_id
));

const shouldSkipDynatraceLookupForProblemId = (context, payload, problemId) => {
    const normalizedProblemId = firstNonEmpty(problemId).toUpperCase();
    if (!normalizedProblemId) {
        return false;
    }

    const configuredSkipPrefixes = normalizeList(firstNonEmpty(
        context.DT_ENRICH_SKIP_PREFIXES,
        payload.dtEnrichSkipPrefixes,
        payload.dt_enrich_skip_prefixes,
        'SMOKE-,TEST-,DEMO-'
    )).map((prefix) => String(prefix || '').trim().toUpperCase()).filter(Boolean);

    return configuredSkipPrefixes.some((prefix) => normalizedProblemId.startsWith(prefix));
};

const parseJsonObject = (value) => {
    if (!value) {
        return null;
    }

    if (typeof value === 'object' && !Array.isArray(value)) {
        return value;
    }

    try {
        const parsed = JSON.parse(String(value));
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
    } catch (error) {
        return null;
    }
};

const resolveAreaTagGroupsFromContext = (context, payload) => {
    const candidates = [
        payload && payload.__areaTagGroups,
        payload && payload.areaTagGroups,
        payload && payload.area_tags_map,
        payload && payload.areaTagsByArea,
        payload && payload.areaTagGroupsJson,
        context && context.AREA_TAG_GROUPS_JSON,
        context && context.AREA_TAG_GROUPS
    ];

    for (const candidate of candidates) {
        const parsed = parseAreaTagGroupsInput(candidate);
        if (parsed) {
            return parsed;
        }
    }

    return DEFAULT_AREA_TAG_GROUPS;
};

const buildIncomingPayload = (event) => {
    const mergedPayload = { ...(event || {}) };
    const possiblePayloads = [event && event.payload, event && event.body, event && event.requestBody, event && event.data, event && event.rawBody];

    possiblePayloads.forEach((candidate) => {
        const parsed = parseJsonObject(candidate);
        if (parsed) {
            Object.assign(mergedPayload, parsed);
        }
    });

    return mergedPayload;
};

const getEvidenceList = (payload) => payload && payload.evidenceDetails && Array.isArray(payload.evidenceDetails.details)
    ? payload.evidenceDetails.details
    : [];

const getEvidencePropertyValue = (payload, propertyKey) => {
    const normalizedKey = String(propertyKey || '').trim().toLowerCase();
    if (!normalizedKey) {
        return '';
    }

    for (const detail of getEvidenceList(payload)) {
        const properties = detail && detail.data && Array.isArray(detail.data.properties)
            ? detail.data.properties
            : [];

        for (const property of properties) {
            if (String(property && property.key || '').trim().toLowerCase() === normalizedKey) {
                return firstNonEmpty(property.value);
            }
        }
    }

    return '';
};

const getEvidenceEntityValue = (payload) => {
    const evidenceList = getEvidenceList(payload);
    const prioritized = evidenceList.slice().sort((left, right) => {
        const leftScore = left && left.rootCauseRelevant ? 1 : 0;
        const rightScore = right && right.rootCauseRelevant ? 1 : 0;
        return rightScore - leftScore;
    });

    for (const detail of prioritized) {
        const entityName = firstNonEmpty(
            extractDisplayValue(detail && detail.entity),
            extractDisplayValue(detail && detail.data && detail.data.entityId)
        );

        if (entityName) {
            return entityName;
        }
    }

    return '';
};

const getImpactAnalysisEntityValue = (payload) => firstNonEmpty(
    extractDisplayValue(payload && payload.impactAnalysis && payload.impactAnalysis.impacts && payload.impactAnalysis.impacts[0] && payload.impactAnalysis.impacts[0].impactedEntity)
);

const getEntityNameCandidates = (payload) => {
    const candidates = [];

    const addCandidate = (value) => {
        const name = cleanForSpeech(extractDisplayValue(value));
        if (name && !candidates.includes(name)) {
            candidates.push(name);
        }
    };

    [
        ...(Array.isArray(payload && payload.affectedEntities) ? payload.affectedEntities : []),
        ...(Array.isArray(payload && payload.impactedEntities) ? payload.impactedEntities : []),
        ...(Array.isArray(payload && payload.impactAnalysis && payload.impactAnalysis.impacts) ? payload.impactAnalysis.impacts.map((impact) => impact.impactedEntity) : []),
        ...(getEvidenceList(payload).map((detail) => detail && detail.entity)),
        ...(getEvidenceList(payload).map((detail) => detail && detail.groupingEntity)),
        payload && payload.rootCauseEntity
    ].forEach(addCandidate);

    return candidates;
};

const pickEntityNameByScore = (candidates, scorer) => {
    let bestCandidate = '';
    let bestScore = Number.NEGATIVE_INFINITY;

    candidates.forEach((candidate) => {
        const score = scorer(candidate);
        if (score > bestScore) {
            bestScore = score;
            bestCandidate = candidate;
        }
    });

    return bestCandidate;
};

const getAffectedUrlValue = (payload) => firstNonEmpty(
    payload.affectedUrl,
    payload.affectedURL,
    getEvidencePropertyValue(payload, 'dt.event.synthetic_affected_urls'),
    getEvidencePropertyValue(payload, 'dt.event.url')
);

const getAffectedLocationValue = (payload) => firstNonEmpty(
    payload.affectedLocation,
    payload.affectedlocation,
    getEvidencePropertyValue(payload, 'dt.event.synthetic_affected_locations')
);

const getMonitorNameValue = (payload) => firstNonEmpty(
    payload.monitorName,
    payload.monitor,
    getEvidencePropertyValue(payload, 'monitor.name'),
    getEvidenceEntityValue(payload)
);

const getWebRequestServiceValue = (payload) => {
    const explicitValue = firstNonEmpty(payload.webRequestService, payload.web_request_service, payload.WebRequestService);
    if (explicitValue) {
        return explicitValue;
    }

    const candidates = getEntityNameCandidates(payload);
    return pickEntityNameByScore(candidates, (candidate) => {
        const text = String(candidate || '').toLowerCase();
        let score = 0;
        if (/tsys/.test(text)) score += 8;
        if (/primeapi/.test(text)) score += 8;
        if (/api/.test(text)) score += 4;
        if (/facade/.test(text)) score += 3;
        if (/controller|adapter/.test(text)) score -= 1;
        return score;
    });
};

const getWebServiceValue = (payload) => {
    const explicitValue = firstNonEmpty(payload.webService, payload.web_service, payload.WebService);
    if (explicitValue) {
        return explicitValue;
    }

    const requestService = cleanForSpeech(getWebRequestServiceValue(payload));
    const candidates = getEntityNameCandidates(payload).filter((candidate) => candidate !== requestService);

    return pickEntityNameByScore(candidates, (candidate) => {
        const text = String(candidate || '').toLowerCase();
        let score = 0;
        if (/\(.*\)/.test(candidate)) score += 8;
        if (/controlleradapter/.test(text)) score += 6;
        if (/adapter/.test(text)) score += 5;
        if (/controller/.test(text)) score += 4;
        if (/tsys|primeapi/.test(text)) score -= 4;
        return score;
    });
};

const getPotentiallyAffectedCallsValue = (payload) => firstNonEmpty(
    payload.numberOfPotentiallyAffectedServiceCalls,
    payload.potentiallyAffectedCalls,
    payload && payload.impactAnalysis && payload.impactAnalysis.impacts && payload.impactAnalysis.impacts[0] && payload.impactAnalysis.impacts[0].numberOfPotentiallyAffectedServiceCalls
);

const getObjectKeys = (value) => value && typeof value === 'object' && !Array.isArray(value)
    ? Object.keys(value).sort()
    : [];

const firstNonEmpty = (...values) => {
    for (const value of values) {
        if (value === null || value === undefined) {
            continue;
        }

        const text = String(value).trim();
        if (text && text.toLowerCase() !== 'null' && text.toLowerCase() !== 'undefined') {
            return text;
        }
    }

    return '';
};

const normalizeLooseKey = (value) => String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');

const getPayloadValueByLooseKey = (payload, ...candidateKeys) => {
    if (!payload || typeof payload !== 'object') {
        return '';
    }

    const normalizedMap = new Map();
    Object.entries(payload).forEach(([key, value]) => {
        const normalizedKey = normalizeLooseKey(key);
        if (normalizedKey && !normalizedMap.has(normalizedKey)) {
            normalizedMap.set(normalizedKey, value);
        }
    });

    for (const candidateKey of candidateKeys) {
        const normalizedCandidate = normalizeLooseKey(candidateKey);
        if (!normalizedCandidate) {
            continue;
        }

        const candidateValue = normalizedMap.get(normalizedCandidate);
        const extracted = firstNonEmpty(extractDisplayValue(candidateValue));
        if (extracted) {
            return extracted;
        }
    }

    return '';
};

const isOutboundDirection = (direction) => {
    const normalized = String(direction || '').toLowerCase();
    return !normalized || normalized.startsWith('outbound');
};

const isSamePhoneNumber = (left, right) => {
    const leftNormalized = normalizePhoneNumber(left);
    const rightNormalized = normalizePhoneNumber(right);

    if (!leftNormalized || !rightNormalized) {
        return false;
    }

    return leftNormalized === rightNormalized;
};

const toCallTimestampMs = (call) => {
    if (!call) {
        return 0;
    }

    const rawTimestamp = call.dateCreated || call.startTime || call.dateUpdated || call.endTime;
    if (!rawTimestamp) {
        return 0;
    }

    if (rawTimestamp instanceof Date) {
        return rawTimestamp.getTime();
    }

    const parsedTimestamp = Date.parse(String(rawTimestamp));
    return Number.isNaN(parsedTimestamp) ? 0 : parsedTimestamp;
};

const toCallAgeSeconds = (call, nowMs = Date.now()) => {
    const createdMs = toCallTimestampMs(call);
    if (!createdMs) {
        return null;
    }

    const ageMs = nowMs - createdMs;
    return ageMs >= 0 ? Math.floor(ageMs / 1000) : null;
};

const toCallTraceSnapshot = (call, nowMs = Date.now()) => {
    if (!call) {
        return null;
    }

    return {
        sid: firstNonEmpty(call.sid),
        status: firstNonEmpty(call.status),
        direction: firstNonEmpty(call.direction),
        from: firstNonEmpty(call.from),
        to: firstNonEmpty(call.to),
        createdAt: firstNonEmpty(call.dateCreated, call.startTime, call.dateUpdated, call.endTime),
        ageSeconds: toCallAgeSeconds(call, nowMs)
    };
};

const buildDuplicateGuardHint = ({ reason, toNumber, fromNumber, windowSeconds, maxResults }) => {
    const normalizedReason = String(reason || '').toLowerCase();
    const destination = firstNonEmpty(toNumber, 'unknown_to');
    const origin = firstNonEmpty(fromNumber, 'unknown_from');
    const window = Math.max(1, asPositiveInt(windowSeconds, 90));
    const max = Math.max(1, asPositiveInt(maxResults, 100));

    if (normalizedReason === 'active_call_in_progress') {
        return `Ya existe una llamada activa al destino ${destination}. Espera cierre de la llamada actual o evita disparos simultáneos de start para el mismo problema.`;
    }

    if (normalizedReason === 'recent_call_in_window') {
        return `Se detectó una llamada reciente para ${destination}. Si este bloqueo es excesivo, disminuye SINGLE_DIAL_PER_LEVEL_WINDOW_SECONDS (actual=${window}) o usa singleDialSmokeWindowSeconds/SINGLE_DIAL_SMOKE_WINDOW_SECONDS para pruebas SMOKE. Si necesitas forzar pruebas controladas, habilita ALLOW_SMOKE_DIAL_GUARD_BYPASS y envía bypassDialGuard=1. Si se escapan duplicados, aumenta la ventana.`;
    }

    if (normalizedReason === 'no_calls_from_expected_origin') {
        return `No se encontraron llamadas salientes de ${origin} hacia ${destination}. Verifica TWILIO_FROM y que el número origen real coincida con la configuración.`;
    }

    if (normalizedReason === 'recent_lookup_failed' || normalizedReason === 'active_lookup_failed') {
        return `La consulta de historial de llamadas falló. Reintenta y valida permisos/API de Twilio; mientras tanto, sube FLOW_LOCK_TTL_SECONDS para mayor protección local.`;
    }

    if (normalizedReason === 'no_duplicate_detected') {
        return `No se detectaron duplicados con la búsqueda actual (maxResults=${max}, windowSeconds=${window}). Si persiste, amplía la ventana o revisa eventos start repetidos en origen.`;
    }

    return `Revisa origen (${origin}) y destino (${destination}) y ajusta ventana de guard si es necesario (windowSeconds=${window}, maxResults=${max}).`;
};

const hasRecentOutboundDial = async ({ client, toNumber, fromNumber, windowSeconds = 90, maxResults = 100 }) => {
    if (!client || !toNumber || !fromNumber) {
        return {
            matched: false,
            reason: 'missing_client_or_numbers',
            hint: buildDuplicateGuardHint({
                reason: 'missing_client_or_numbers',
                toNumber,
                fromNumber,
                windowSeconds,
                maxResults
            })
        };
    }

    const normalizedWindow = Math.max(1, asPositiveInt(windowSeconds, 90));
    const normalizedMaxResults = Math.max(1, asPositiveInt(maxResults, 100));
    const normalizedFromNumber = normalizePhoneNumber(fromNumber);
    const now = Date.now();

    try {
        const recentCalls = await client.calls.list({
            to: toNumber,
            limit: normalizedMaxResults
        });

        if (!Array.isArray(recentCalls) || !recentCalls.length) {
            return {
                matched: false,
                reason: 'no_calls_for_destination',
                stats: {
                    fetchedCount: 0,
                    outboundCount: 0,
                    fromMatchCount: 0,
                    windowSeconds: normalizedWindow,
                    maxResults: normalizedMaxResults
                },
                hint: buildDuplicateGuardHint({
                    reason: 'no_duplicate_detected',
                    toNumber,
                    fromNumber,
                    windowSeconds: normalizedWindow,
                    maxResults: normalizedMaxResults
                })
            };
        }

        const outboundCalls = recentCalls.filter((call) => isOutboundDirection(call && call.direction));
        const matchingFromCalls = normalizedFromNumber
            ? outboundCalls.filter((call) => isSamePhoneNumber(call && call.from, normalizedFromNumber))
            : outboundCalls;

        const stats = {
            fetchedCount: recentCalls.length,
            outboundCount: outboundCalls.length,
            fromMatchCount: matchingFromCalls.length,
            windowSeconds: normalizedWindow,
            maxResults: normalizedMaxResults
        };

        if (!matchingFromCalls.length) {
            return {
                matched: false,
                reason: 'no_calls_from_expected_origin',
                stats,
                sampleCalls: outboundCalls.slice(0, 3).map((call) => toCallTraceSnapshot(call, now)).filter(Boolean),
                hint: buildDuplicateGuardHint({
                    reason: 'no_calls_from_expected_origin',
                    toNumber,
                    fromNumber,
                    windowSeconds: normalizedWindow,
                    maxResults: normalizedMaxResults
                })
            };
        }

        const recentMatch = matchingFromCalls.find((call) => {
            const callCreatedMs = toCallTimestampMs(call);
            if (!callCreatedMs) {
                return false;
            }

            const ageMs = now - callCreatedMs;
            return ageMs >= 0 && ageMs <= normalizedWindow * 1000;
        });

        if (recentMatch) {
            const ageSeconds = toCallAgeSeconds(recentMatch, now);
            console.log(`Recent dial guard matched call ${recentMatch.sid || 'unknown'} status=${recentMatch.status || 'unknown'} ageSeconds=${ageSeconds === null ? 'unknown' : ageSeconds}`);
            return {
                matched: true,
                reason: 'recent_call_in_window',
                source: 'recent',
                matchedCall: toCallTraceSnapshot(recentMatch, now),
                stats,
                hint: buildDuplicateGuardHint({
                    reason: 'recent_call_in_window',
                    toNumber,
                    fromNumber,
                    windowSeconds: normalizedWindow,
                    maxResults: normalizedMaxResults
                })
            };
        }

        return {
            matched: false,
            reason: 'no_recent_call_within_window',
            source: 'recent',
            stats,
            sampleCalls: matchingFromCalls.slice(0, 3).map((call) => toCallTraceSnapshot(call, now)).filter(Boolean),
            hint: buildDuplicateGuardHint({
                reason: 'no_duplicate_detected',
                toNumber,
                fromNumber,
                windowSeconds: normalizedWindow,
                maxResults: normalizedMaxResults
            })
        };
    } catch (error) {
        console.log(`Recent dial guard lookup failed for ${toNumber}: ${error.message}`);
        return {
            matched: false,
            reason: 'recent_lookup_failed',
            source: 'recent',
            error: truncateText(error.message || 'unknown_error', 220),
            hint: buildDuplicateGuardHint({
                reason: 'recent_lookup_failed',
                toNumber,
                fromNumber,
                windowSeconds: normalizedWindow,
                maxResults: normalizedMaxResults
            })
        };
    }
};

const hasActiveOutboundDial = async ({ client, toNumber, fromNumber, maxResults = 100 }) => {
    if (!client || !toNumber || !fromNumber) {
        return {
            matched: false,
            reason: 'missing_client_or_numbers',
            source: 'active',
            hint: buildDuplicateGuardHint({
                reason: 'missing_client_or_numbers',
                toNumber,
                fromNumber,
                windowSeconds: 0,
                maxResults
            })
        };
    }

    const normalizedMaxResults = Math.max(1, asPositiveInt(maxResults, 100));
    const normalizedFromNumber = normalizePhoneNumber(fromNumber);
    const statusesToCheck = ['queued', 'ringing', 'in-progress'];
    const statusCounts = {};
    const now = Date.now();

    try {
        for (const status of statusesToCheck) {
            const activeCalls = await client.calls.list({
                to: toNumber,
                status,
                limit: normalizedMaxResults
            });
            statusCounts[status] = Array.isArray(activeCalls) ? activeCalls.length : 0;

            const matchingActive = Array.isArray(activeCalls)
                ? activeCalls.find((call) => {
                    if (!isOutboundDirection(call && call.direction)) {
                        return false;
                    }

                    if (!normalizedFromNumber) {
                        return true;
                    }

                    return isSamePhoneNumber(call && call.from, normalizedFromNumber);
                })
                : null;

            if (matchingActive) {
                const firstActive = matchingActive;
                console.log(`Active dial guard matched call ${firstActive.sid || 'unknown'} status=${firstActive.status || status}`);
                return {
                    matched: true,
                    reason: 'active_call_in_progress',
                    source: 'active',
                    matchedCall: toCallTraceSnapshot(firstActive, now),
                    stats: {
                        statusCounts,
                        maxResults: normalizedMaxResults
                    },
                    hint: buildDuplicateGuardHint({
                        reason: 'active_call_in_progress',
                        toNumber,
                        fromNumber,
                        windowSeconds: 0,
                        maxResults: normalizedMaxResults
                    })
                };
            }
        }

        return {
            matched: false,
            reason: 'no_active_calls',
            source: 'active',
            stats: {
                statusCounts,
                maxResults: normalizedMaxResults
            },
            hint: buildDuplicateGuardHint({
                reason: 'no_duplicate_detected',
                toNumber,
                fromNumber,
                windowSeconds: 0,
                maxResults: normalizedMaxResults
            })
        };
    } catch (error) {
        console.log(`Active dial guard lookup failed for ${toNumber}: ${error.message}`);
        return {
            matched: false,
            reason: 'active_lookup_failed',
            source: 'active',
            error: truncateText(error.message || 'unknown_error', 220),
            hint: buildDuplicateGuardHint({
                reason: 'active_lookup_failed',
                toNumber,
                fromNumber,
                windowSeconds: 0,
                maxResults: normalizedMaxResults
            })
        };
    }
};

const hasExistingOutboundDial = async ({ client, toNumber, fromNumber, windowSeconds = 90, maxResults = 100, includeRecentLookup = true }) => {
    const activeResult = await hasActiveOutboundDial({
        client,
        toNumber,
        fromNumber,
        maxResults
    });

    if (activeResult.matched) {
        return {
            matched: true,
            reason: 'active_call_in_progress',
            source: 'active',
            matchedCall: activeResult.matchedCall || null,
            hint: firstNonEmpty(activeResult.hint),
            diagnostics: {
                activeReason: firstNonEmpty(activeResult.reason),
                recentReason: '',
                activeStats: activeResult.stats || null,
                recentStats: null
            }
        };
    }

    if (!includeRecentLookup) {
        return {
            matched: false,
            reason: 'no_duplicate_detected',
            source: 'active_only',
            hint: buildDuplicateGuardHint({
                reason: 'no_duplicate_detected',
                toNumber,
                fromNumber,
                windowSeconds,
                maxResults
            }),
            diagnostics: {
                activeReason: firstNonEmpty(activeResult.reason),
                recentReason: 'recent_lookup_skipped',
                activeStats: activeResult.stats || null,
                recentStats: null,
                activeError: firstNonEmpty(activeResult.error),
                recentError: ''
            }
        };
    }

    const recentResult = await hasRecentOutboundDial({
        client,
        toNumber,
        fromNumber,
        windowSeconds,
        maxResults
    });

    if (recentResult.matched) {
        return {
            matched: true,
            reason: 'recent_call_in_window',
            source: 'recent',
            matchedCall: recentResult.matchedCall || null,
            hint: firstNonEmpty(recentResult.hint),
            diagnostics: {
                activeReason: firstNonEmpty(activeResult.reason),
                recentReason: firstNonEmpty(recentResult.reason),
                activeStats: activeResult.stats || null,
                recentStats: recentResult.stats || null
            }
        };
    }

    return {
        matched: false,
        reason: 'no_duplicate_detected',
        source: 'none',
        hint: buildDuplicateGuardHint({
            reason: 'no_duplicate_detected',
            toNumber,
            fromNumber,
            windowSeconds,
            maxResults
        }),
        diagnostics: {
            activeReason: firstNonEmpty(activeResult.reason),
            recentReason: firstNonEmpty(recentResult.reason),
            activeStats: activeResult.stats || null,
            recentStats: recentResult.stats || null,
            activeError: firstNonEmpty(activeResult.error),
            recentError: firstNonEmpty(recentResult.error)
        }
    };
};

const extractDisplayValue = (value) => {
    if (value === null || value === undefined) {
        return '';
    }

    if (Array.isArray(value)) {
        return value.map((item) => extractDisplayValue(item)).filter(Boolean).join(', ');
    }

    if (typeof value === 'object') {
        return firstNonEmpty(
            value.stringRepresentation,
            value.name,
            value.displayName,
            value.entityName,
            value.entityId,
            value.title,
            value.value,
            value.id,
            value.problemId,
            value.displayId
        );
    }

    return cleanForSpeech(value);
};

const normalizeTagSource = (source) => {
    if (!source) {
        return [];
    }

    if (Array.isArray(source)) {
        return source;
    }

    if (typeof source === 'string') {
        return source.split(',').map((item) => item.trim()).filter(Boolean);
    }

    return [source];
};

const DEFAULT_AREA_TAG_GROUPS = Object.freeze({
    'Area SRE': [
        'custom_call_turno_observabilidad',
        'custom:call_turno_observabilidad',
        'call_turno_observabilidad'
    ],
    'Area Programacion': [
        'custom_programacion',
        'custom:_programacion',
        'call_turno_progra'
    ]
});

const parseAreaTagGroupsInput = (value) => {
    if (!value) {
        return null;
    }

    let parsed = value;
    if (typeof value === 'string') {
        try {
            parsed = JSON.parse(value);
        } catch (error) {
            return null;
        }
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return null;
    }

    const normalizedGroups = {};
    Object.entries(parsed).forEach(([areaName, tags]) => {
        const normalizedAreaName = String(areaName || '').trim();
        if (!normalizedAreaName) {
            return;
        }

        const normalizedTags = normalizeTagSource(tags)
            .map((tag) => String(tag || '').trim())
            .filter(Boolean);

        if (normalizedTags.length) {
            normalizedGroups[normalizedAreaName] = normalizedTags;
        }
    });

    return Object.keys(normalizedGroups).length ? normalizedGroups : null;
};

const getAreaTagGroups = (payload) => {
    const candidates = [
        payload && payload.__areaTagGroups,
        payload && payload.areaTagGroups,
        payload && payload.area_tags_map,
        payload && payload.areaTagsByArea,
        payload && payload.areaTagGroupsJson
    ];

    for (const candidate of candidates) {
        const parsed = parseAreaTagGroupsInput(candidate);
        if (parsed) {
            return parsed;
        }
    }

    return DEFAULT_AREA_TAG_GROUPS;
};

const normalizeTagForMatching = (tagValue) => {
    const normalizedTag = String(tagValue || '')
        .trim()
        .toLowerCase()
        .replace(/[\s\-./]+/g, '_')
        .replace(/:+/g, ':');

    return {
        normalizedTag,
        normalizedTagLoose: normalizedTag.replace(/[:_]/g, '')
    };
};

const getPayloadNormalizedTags = (payload) => {
    const tagSources = [
        payload.entityTags,
        payload.EntityTags,
        payload.tags,
        payload.entity_tags,
        payload['Entity tags'],
        payload['entity tags'],
        payload.entityTag,
        payload.tag
    ].filter(Boolean);

    const normalizedTags = [];

    tagSources.forEach((source) => {
        normalizeTagSource(source).forEach((tag) => {
            const tagText = typeof tag === 'object'
                ? firstNonEmpty(tag.stringRepresentation, tag.key, tag.value, tag.name, tag.tag, `${tag.context || ''}:${tag.key || ''}`)
                : String(tag || '');

            if (!tagText) {
                return;
            }

            normalizedTags.push(normalizeTagForMatching(tagText));
        });
    });

    return normalizedTags;
};

const getAreaTagMatches = (payload) => {
    const areaTagGroups = getAreaTagGroups(payload);
    const payloadTags = getPayloadNormalizedTags(payload);
    const matches = {};

    Object.entries(areaTagGroups).forEach(([areaName, areaNeedles]) => {
        const areaMatches = [];

        areaNeedles.forEach((needle) => {
            const normalizedNeedle = String(needle || '').trim().toLowerCase();
            if (!normalizedNeedle) {
                return;
            }

            const normalizedNeedleLoose = normalizedNeedle.replace(/[:_]/g, '');
            const matched = payloadTags.some((tagInfo) => {
                return tagInfo.normalizedTag.includes(normalizedNeedle)
                    || tagInfo.normalizedTagLoose.includes(normalizedNeedleLoose);
            });

            if (matched) {
                areaMatches.push(needle);
            }
        });

        if (areaMatches.length) {
            matches[areaName] = areaMatches;
        }
    });

    return matches;
};

const getResolvedIncidentArea = (payload) => {
    const explicitArea = firstNonEmpty(
        payload.incidentArea,
        payload.incident_area,
        payload.area,
        payload.areaName,
        payload.area_name,
        payload.areaCode,
        payload.area_code
    );

    if (explicitArea) {
        return cleanForSpeech(explicitArea);
    }

    const areaMatches = getAreaTagMatches(payload);
    const firstMatchedArea = Object.keys(areaMatches)[0] || '';
    return cleanForSpeech(firstMatchedArea);
};

const hasObservabilityCriticalTag = (payload) => {
    return Object.keys(getAreaTagMatches(payload)).length > 0;
};

const getNormalizedSeverity = (payload) => firstNonEmpty(
    payload.severity,
    payload.Severity,
    payload.severityLevel,
    payload.severidad,
    payload.Severidad,
    payload.eventCategory,
    payload['event.category'],
    payload.ProblemSeverity,
    payload.PROBLEM_SEVERITY,
    payload.alertSeverity
).toUpperCase();

const getNormalizedCriticality = (payload) => firstNonEmpty(
    payload.criticality,
    payload.Criticality,
    payload.criticidad,
    payload.Criticidad
).toUpperCase();

const getEffectiveCriticality = (payload) => {
    const hasRequiredTag = hasObservabilityCriticalTag(payload);
    const explicitCriticality = getNormalizedCriticality(payload);

    if (!hasRequiredTag) {
        return '';
    }

    if (explicitCriticality) {
        return explicitCriticality === 'CRITICAL' ? 'CRITICAL' : '';
    }

    return 'CRITICAL';
};

const getRootCauseValue = (payload) => firstNonEmpty(
    getPayloadValueByLooseKey(payload, 'ProblemRootCause', 'problemRootCause', 'problem_root_cause', 'problemrootcause'),
    extractDisplayValue(payload.ProblemRootCause),
    extractDisplayValue(payload.problemRootCause),
    extractDisplayValue(payload.problem_root_cause),
    extractDisplayValue(payload.problemrootcause),
    extractDisplayValue(payload.Causa),
    extractDisplayValue(payload.causa),
    extractDisplayValue(payload.causeName),
    extractDisplayValue(payload.CauseName),
    extractDisplayValue(payload.rootCauseName),
    extractDisplayValue(payload.rootCause),
    extractDisplayValue(payload.rootCauseEntityName),
    extractDisplayValue(payload.RootCause),
    extractDisplayValue(payload['Root cause']),
    extractDisplayValue(payload['root cause']),
    extractDisplayValue(payload.root_cause),
    extractDisplayValue(payload['event.rootCause']),
    extractDisplayValue(payload['event.root_cause']),
    extractDisplayValue(payload.rootCauseEntity),
    extractDisplayValue(payload.rootCauseEntityId),
    extractDisplayValue(payload.rootCauseEntityName),
    getEvidencePropertyValue(payload, 'dt.event.description'),
    getEvidenceEntityValue(payload)
);

const getAffectedEntityValue = (payload) => firstNonEmpty(
    getPayloadValueByLooseKey(payload, 'ProblemImpact', 'problemImpact', 'problem_impact', 'problemimpact'),
    extractDisplayValue(payload.ProblemImpact),
    extractDisplayValue(payload.problemImpact),
    extractDisplayValue(payload.problem_impact),
    extractDisplayValue(payload.problemimpact),
    extractDisplayValue(payload.ImpactedEntity),
    extractDisplayValue(payload.impactedEntity),
    extractDisplayValue(payload.EntidadAfectada),
    extractDisplayValue(payload.entidadAfectada),
    extractDisplayValue(payload.affectedEntity),
    extractDisplayValue(payload.affected_entity_names),
    extractDisplayValue(payload.affectedEntities),
    extractDisplayValue(payload['Affected entities']),
    extractDisplayValue(payload['affected entities']),
    extractDisplayValue(payload.impactedEntities),
    extractDisplayValue(payload['Impacted entities']),
    extractDisplayValue(payload['impacted entities']),
    extractDisplayValue(payload['event.affected_entity_names']),
    getEvidenceEntityValue(payload),
    getImpactAnalysisEntityValue(payload)
);

const extractProblemIdFromUrl = (rawUrl) => {
    const problemUrl = firstNonEmpty(rawUrl);
    if (!problemUrl) {
        return '';
    }

    const pidMatch = problemUrl.match(/[?;]pid=([^&;#]+)/i);
    if (pidMatch && pidMatch[1]) {
        try {
            return cleanForSpeech(decodeURIComponent(pidMatch[1]));
        } catch (error) {
            return cleanForSpeech(pidMatch[1]);
        }
    }

    const pathMatch = problemUrl.match(/\/problems\/(?:problemdetails;pid=)?([^/?#;]+)/i);
    if (pathMatch && pathMatch[1]) {
        try {
            return cleanForSpeech(decodeURIComponent(pathMatch[1]));
        } catch (error) {
            return cleanForSpeech(pathMatch[1]);
        }
    }

    return '';
};

const resolveProblemId = (payload) => {
    const explicitProblemId = firstNonEmpty(
        payload.problemId,
        payload.problem_id,
        payload.ProblemID,
        payload.PROBLEM_ID,
        payload.problemID,
        payload['problem.id'],
        payload['problem_id'],
        payload['event.problemId'],
        payload['event.problem_id'],
        payload.event && payload.event.problemId,
        payload.event && payload.event.problem_id,
        extractProblemIdFromUrl(firstNonEmpty(payload.problemUrl, payload.ProblemURL, payload.url, payload.problemURL)),
        payload.problemDisplayId,
        payload.displayId,
        payload.DisplayID,
        payload.displayID,
        payload.display_id,
        payload['event.id'],
        payload.event && payload.event.id
    );

    if (explicitProblemId) {
        return cleanForSpeech(explicitProblemId).toUpperCase();
    }

    const stableDisplayId = firstNonEmpty(payload.displayId, payload.DisplayID, payload.displayID, payload.display_id, payload.problemDisplayId).toUpperCase();
    const stableProblemUrl = extractProblemIdFromUrl(firstNonEmpty(payload.problemUrl, payload.ProblemURL, payload.url, payload.problemURL)).toUpperCase();

    const fingerprintSource = [
        stableDisplayId,
        stableProblemUrl,
        firstNonEmpty(payload.title, payload.Titulo, payload.ProblemTitle, payload.problemTitle, payload.alertTitle, payload['event.name']),
        getRootCauseValue(payload),
        getAffectedEntityValue(payload),
        getAffectedUrlValue(payload),
        getMonitorNameValue(payload),
        firstNonEmpty(payload.webRequestService, payload.webService)
    ]
        .map((item) => cleanForSpeech(item).toLowerCase())
        .filter(Boolean)
        .join('|');

    if (!fingerprintSource) {
        return 'unknown';
    }

    const digest = crypto.createHash('sha1').update(fingerprintSource).digest('hex').slice(0, 16);
    return `fp-${digest}`;
};

const buildDynatraceProblemUrl = (baseUrl, problemId) => {
    if (!baseUrl || !problemId) {
        return '';
    }

    return `${String(baseUrl).replace(/\/$/, '')}/#problems/problemdetails;pid=${encodeURIComponent(problemId)}`;
};

const getDynatraceBaseUrl = (context, payload) => {
    const explicitUrl = firstNonEmpty(context.DT_BASE_URL, payload.dtBaseUrl, payload.baseUrl);
    if (explicitUrl) {
        return explicitUrl;
    }

    const problemUrl = firstNonEmpty(payload.problemUrl, payload.ProblemURL, payload.url, payload.problemURL);
    if (!problemUrl) {
        return '';
    }

    try {
        const parsed = new URL(problemUrl);
        return `${parsed.protocol}//${parsed.host}`;
    } catch (error) {
        return '';
    }
};

const fetchDynatraceProblemDetails = async (context, payload, problemId) => {
    const baseUrl = getDynatraceBaseUrl(context, payload);
    const apiToken = firstNonEmpty(context.DT_API_TOKEN, payload.dtApiToken);

    if (!baseUrl || !apiToken || !problemId) {
        return null;
    }

    const response = await fetch(`${String(baseUrl).replace(/\/$/, '')}/api/v2/problems/${encodeURIComponent(problemId)}`, {
        method: 'GET',
        headers: {
            Authorization: `Api-Token ${apiToken}`,
            Accept: 'application/json'
        }
    });

    if (!response.ok) {
        const body = await response.text();
        throw new Error(`Dynatrace problem lookup failed with status ${response.status}: ${body}`);
    }

    return response.json();
};

const enrichPayloadWithDynatraceProblem = (payload, problemDetails, context) => {
    if (!problemDetails) {
        return payload;
    }

    const firstAffectedEntity = Array.isArray(problemDetails.affectedEntities) ? problemDetails.affectedEntities[0] : null;
    const firstImpactedEntity = Array.isArray(problemDetails.impactedEntities) ? problemDetails.impactedEntities[0] : null;
    const managementZone = Array.isArray(problemDetails.managementZones) ? problemDetails.managementZones[0] : problemDetails.managementZones;
    const baseUrl = getDynatraceBaseUrl(context, payload);

    return {
        ...problemDetails,
        ...payload,
        problemId: firstNonEmpty(payload.problemId, payload.ProblemID, payload['event.id'], problemDetails.problemId),
        ProblemID: firstNonEmpty(payload.ProblemID, payload.problemId, payload['event.id'], problemDetails.problemId),
        displayId: firstNonEmpty(payload.displayId, payload.DisplayID, payload.display_id, problemDetails.displayId),
        title: firstNonEmpty(payload.title, payload.Titulo, payload.ProblemTitle, payload['event.name'], problemDetails.title),
        ProblemTitle: firstNonEmpty(payload.ProblemTitle, payload.title, problemDetails.title),
        status: firstNonEmpty(payload.status, payload.State, payload['event.status'], problemDetails.status),
        Severity: firstNonEmpty(payload.Severity, payload.severity, problemDetails.severity, problemDetails.severityLevel),
        severity: firstNonEmpty(payload.severity, payload.Severity, problemDetails.severity, problemDetails.severityLevel),
        severityLevel: firstNonEmpty(payload.severityLevel, problemDetails.severityLevel, problemDetails.severity),
        serviceName: firstNonEmpty(payload.serviceName, payload.Servicio, getRootCauseValue(payload), extractDisplayValue(problemDetails.rootCause), extractDisplayValue(problemDetails.rootCauseEntity), problemDetails.title),
        affectedEntity: firstNonEmpty(getAffectedEntityValue(payload), extractDisplayValue(firstAffectedEntity), extractDisplayValue(firstImpactedEntity)),
        rootCauseName: firstNonEmpty(payload.rootCauseName, getRootCauseValue(payload), extractDisplayValue(problemDetails.rootCause), extractDisplayValue(problemDetails.rootCauseEntity)),
        causeName: firstNonEmpty(payload.causeName, payload.CauseName, getRootCauseValue(payload), getRootCauseValue(problemDetails)),
        managementZone: firstNonEmpty(payload.managementZone, extractDisplayValue(managementZone)),
        managementZones: payload.managementZones || problemDetails.managementZones,
        impactLevel: firstNonEmpty(payload.impactLevel, problemDetails.impactLevel),
        details: firstNonEmpty(payload.details, payload.Detalles, payload['event.description'], getEvidencePropertyValue(problemDetails, 'dt.event.description'), `${problemDetails.status || 'OPEN'} Problem ${problemDetails.displayId || ''}`.trim()),
        problemUrl: firstNonEmpty(payload.problemUrl, payload.ProblemURL, buildDynatraceProblemUrl(baseUrl, problemDetails.problemId)),
        entityTags: payload.entityTags || payload.EntityTags || payload.entity_tags || payload['Entity tags'] || problemDetails.entityTags,
        monitorName: firstNonEmpty(payload.monitorName, getMonitorNameValue(payload), getMonitorNameValue(problemDetails)),
        webRequestService: firstNonEmpty(payload.webRequestService, getWebRequestServiceValue(payload), getWebRequestServiceValue(problemDetails)),
        webService: firstNonEmpty(payload.webService, getWebServiceValue(payload), getWebServiceValue(problemDetails)),
        affectedUrl: firstNonEmpty(payload.affectedUrl, getAffectedUrlValue(payload), getAffectedUrlValue(problemDetails)),
        affectedLocation: firstNonEmpty(payload.affectedLocation, getAffectedLocationValue(payload), getAffectedLocationValue(problemDetails)),
        potentiallyAffectedCalls: firstNonEmpty(payload.potentiallyAffectedCalls, getPotentiallyAffectedCallsValue(payload), getPotentiallyAffectedCallsValue(problemDetails)),
        criticality: getEffectiveCriticality({ ...problemDetails, ...payload })
    };
};

const requiresDynatraceEnrichment = (payload) => {
    const severity = getNormalizedSeverity(payload);
    const criticality = getEffectiveCriticality(payload);
    const title = firstNonEmpty(payload.title, payload.Titulo, payload.ProblemTitle, payload.problemTitle);
    const rootCause = getRootCauseValue(payload);
    const affectedEntity = getAffectedEntityValue(payload);
    return !severity || !criticality || !title || !rootCause || !affectedEntity;
};

const isCriticalAlert = (payload, incidentSummary) => {
    const hasRequiredTag = hasObservabilityCriticalTag(payload);
    const criticality = getEffectiveCriticality(payload);

    return hasRequiredTag && criticality === 'CRITICAL';
};

const shouldNotifyForStatus = (context, payload) => {
    const notifyOnlyOpen = asBooleanFlag(firstNonEmpty(context.NOTIFY_ONLY_OPEN, payload.notifyOnlyOpen, 'true'), true);
    if (!notifyOnlyOpen) {
        return true;
    }

    const normalizedStatus = firstNonEmpty(payload.status, payload.State, payload.state, payload.problemStatus, payload['event.status']).toUpperCase();
    if (!normalizedStatus) {
        return true;
    }

    return ['OPEN', 'ACTIVE', 'PROBLEM_OPEN'].some((allowed) => normalizedStatus.includes(allowed));
};

const toReadableSeverity = (severity) => {
    const normalized = String(severity || '').toUpperCase();
    const severityMap = {
        CRITICAL: 'Crítica',
        WARNING: 'Advertencia',
        AVAILABILITY: 'Disponibilidad',
        ERROR: 'Error',
        PERFORMANCE: 'Performance',
        RESOURCE_CONTENTION: 'Recursos',
        CUSTOM_ALERT: 'Alerta personalizada',
        MONITORING_UNAVAILABLE: 'Monitoreo no disponible',
        SECURITY: 'Seguridad'
    };

    return severityMap[normalized] || (severity ? cleanForSpeech(severity) : 'Sin severidad');
};

const deriveCategory = (severity) => {
    const normalized = String(severity || '').toUpperCase();

    if (normalized === 'CRITICAL') {
        return 'Crítica';
    }

    if (normalized === 'WARNING') {
        return 'Advertencia';
    }

    if (['AVAILABILITY', 'ERROR', 'MONITORING_UNAVAILABLE'].includes(normalized)) {
        return 'Disponibilidad';
    }

    if (['PERFORMANCE', 'RESOURCE_CONTENTION'].includes(normalized)) {
        return 'Performance';
    }

    if (normalized === 'SECURITY') {
        return 'Seguridad';
    }

    if (normalized === 'CUSTOM_ALERT') {
        return 'Alerta personalizada';
    }

    return 'General';
};

const derivePriority = (severity, impactLevel, estimatedUsers) => {
    const normalizedSeverity = String(severity || '').toUpperCase();
    const normalizedImpact = String(impactLevel || '').toUpperCase();
    const users = Number(estimatedUsers || 0);

    if (normalizedSeverity === 'CRITICAL') {
        return 'P1';
    }

    if (normalizedSeverity === 'WARNING') {
        return 'P3';
    }

    if (['AVAILABILITY', 'ERROR', 'MONITORING_UNAVAILABLE', 'SECURITY'].includes(normalizedSeverity)) {
        return 'P1';
    }

    if (normalizedSeverity === 'PERFORMANCE' && (normalizedImpact === 'APPLICATION' || normalizedImpact === 'APPLICATIONS' || users >= 50)) {
        return 'P2';
    }

    if (['PERFORMANCE', 'RESOURCE_CONTENTION', 'CUSTOM_ALERT'].includes(normalizedSeverity)) {
        return 'P3';
    }

    return 'P4';
};

const buildIncidentSummary = (event, problemId) => {
    const displayId = cleanForSpeech(firstNonEmpty(event.displayId, event.DisplayID, event.displayID, event.display_id, event.problemDisplayId, event.problemId, event.ProblemID, event['event.id'], problemId) || problemId);
    const title = cleanForSpeech(firstNonEmpty(event.title, event.Titulo, event.titulo, event.ProblemTitle, event.problemTitle, event.alertTitle, event['event.name']) || 'Incidente sin título');
    const status = cleanForSpeech(firstNonEmpty(event.status, event.State, event.state, event.problemStatus, event['event.status']) || 'OPEN');
    const severity = getNormalizedSeverity(event);
    const criticality = getEffectiveCriticality(event);
    const readableSeverity = toReadableSeverity(severity);
    const impactLevel = cleanForSpeech(firstNonEmpty(event.impactLevel, event.ImpactLevel, event.IMPACT_LEVEL, event.impact, event.eventType));
    const rootCause = cleanForSpeech(getRootCauseValue(event));
    const causeName = cleanForSpeech(firstNonEmpty(
        extractDisplayValue(event.causeName),
        extractDisplayValue(event.CauseName),
        rootCause,
        extractDisplayValue(event.serviceName),
        extractDisplayValue(event.Servicio),
        extractDisplayValue(event.servicio)
    ));
    const affectedEntity = cleanForSpeech(getAffectedEntityValue(event));
    const incidentArea = cleanForSpeech(getResolvedIncidentArea(event));
    const monitorName = cleanForSpeech(firstNonEmpty(extractDisplayValue(event.monitorName), getMonitorNameValue(event)));
    const webRequestService = cleanForSpeech(firstNonEmpty(extractDisplayValue(event.webRequestService), getWebRequestServiceValue(event)));
    const webService = cleanForSpeech(firstNonEmpty(extractDisplayValue(event.webService), getWebServiceValue(event)));
    const affectedUrl = cleanForSpeech(firstNonEmpty(extractDisplayValue(event.affectedUrl), getAffectedUrlValue(event)));
    const affectedLocation = cleanForSpeech(firstNonEmpty(extractDisplayValue(event.affectedLocation), getAffectedLocationValue(event)));
    const managementZone = cleanForSpeech(firstNonEmpty(
        extractDisplayValue(event.managementZone),
        extractDisplayValue(event.managementZones),
        extractDisplayValue(event.zone)
    ));
    const potentiallyAffectedCalls = cleanForSpeech(firstNonEmpty(event.potentiallyAffectedCalls, getPotentiallyAffectedCallsValue(event)));
    const estimatedUsers = event.estimatedAffectedUsers || event.affectedUsers || event.usersImpacted || 0;
    const category = cleanForSpeech(event.category || deriveCategory(severity));
    const priority = cleanForSpeech(event.priority || (criticality === 'CRITICAL' ? 'P1' : derivePriority(severity, impactLevel, estimatedUsers)));
    const problemUrl = cleanForSpeech(firstNonEmpty(event.problemUrl, event.ProblemURL, event.url, event.problemURL));
    const details = cleanForSpeech(firstNonEmpty(event.Detalles, event.detalles, event.details, event.problemDetails, event['event.description'], getEvidencePropertyValue(event, 'dt.event.description')));
    const compactDetails = getCompactDetails(details, 170);
    const extendedDetails = buildDescriptiveDetails({
        title,
        causeName,
        affectedEntity,
        details,
        status,
        affectedUrl
    });

    const voiceParts = [
        `Alerta Dynatrace prioridad ${priority}.`,
        `Categoría ${category}.`,
        criticality ? `Criticidad ${criticality}.` : '',
        `Incidente ${displayId}.`,
        title ? `${title}.` : '',
        incidentArea ? `Área ${incidentArea}.` : '',
        causeName ? `Causa ${causeName}.` : '',
        affectedEntity ? `Entidad afectada ${affectedEntity}.` : '',
        webRequestService ? `Web request service ${webRequestService}.` : '',
        webService ? `Web service ${webService}.` : '',
        readableSeverity ? `Severidad ${readableSeverity}.` : '',
        compactDetails ? `${compactDetails}.` : '',
        managementZone ? `Zona ${managementZone}.` : '',
        `Estado ${status}.`
    ].filter(Boolean);

    const smsParts = [
        `[Dynatrace ${priority}/${category}]`,
        problemId ? `ID: ${problemId}` : '',
        title,
        incidentArea ? `Area: ${incidentArea}` : '',
        criticality ? `Criticidad: ${criticality}` : '',
        causeName ? `Causa: ${causeName}` : '',
        affectedEntity ? `Entidad: ${affectedEntity}` : '',
        webRequestService ? `Web request service: ${webRequestService}` : '',
        webService ? `Web service: ${webService}` : '',
        readableSeverity ? `Severidad: ${readableSeverity}` : '',
        status ? `Estado: ${status}` : '',
        compactDetails ? `Detalles: ${compactDetails}` : '',
        problemUrl ? `URL: ${problemUrl}` : ''
    ].filter(Boolean);

    return {
        priority,
        category,
        criticality,
        title,
        serviceName: causeName,
        causeName,
        affectedEntity,
        incidentArea,
        monitorName,
        webRequestService,
        webService,
        affectedUrl,
        affectedLocation,
        managementZone,
        impactLevel,
        potentiallyAffectedCalls,
        compactDetails,
        extendedDetails,
        details,
        voiceMessage: cleanForSpeech(voiceParts.join(' ')),
        smsMessage: cleanForSpeech(smsParts.join(' | '))
    };
};

const shouldRetryCall = (callStatus) => {
    const normalizedStatus = String(callStatus || '').toLowerCase();
    return ['busy', 'failed', 'no-answer', 'canceled'].includes(normalizedStatus);
};

const isAnsweredCall = (callStatus) => {
    const normalizedStatus = String(callStatus || '').toLowerCase();
    return normalizedStatus === 'completed';
};

const normalizeAnsweredBy = (answeredBy) => String(answeredBy || '').trim().toLowerCase();

const classifyAmdResult = (answeredBy) => {
    const normalized = normalizeAnsweredBy(answeredBy);

    if (normalized === 'human') {
        return 'human';
    }

    if (normalized.startsWith('machine') || normalized === 'fax') {
        return 'robot';
    }

    return 'unknown';
};

const shouldTreatCallAsHuman = (callStatus, answeredBy) => {
    return isAnsweredCall(callStatus) && classifyAmdResult(answeredBy) === 'human';
};

const shouldContinueEscalation = (callStatus, answeredBy) => {
    if (shouldRetryCall(callStatus)) {
        return true;
    }

    return isAnsweredCall(callStatus) && classifyAmdResult(answeredBy) !== 'human';
};

const buildAmdAuditRecord = ({ problemId, callSid, level, attempt, levelIndex, toNumber, callStatus, answeredBy, source }) => ({
    source: source || 'amd',
    problemId,
    callSid: callSid || '',
    level,
    attempt,
    levelIndex,
    toNumber: toNumber || '',
    callStatus: String(callStatus || '').toLowerCase(),
    answeredBy: answeredBy || '',
    amdClassification: classifyAmdResult(answeredBy),
    auditedAt: new Date().toISOString()
});

const buildUrlWithParams = (baseUrl, params = {}) => {
    const rawBase = firstNonEmpty(baseUrl);
    if (!rawBase) {
        return '';
    }

    const upsertParams = (searchParams) => {
        Object.entries(params || {}).forEach(([key, value]) => {
            if (value === null || value === undefined || String(value).trim() === '') {
                searchParams.delete(key);
            } else {
                searchParams.set(key, String(value));
            }
        });
    };

    try {
        const parsed = new URL(rawBase);
        upsertParams(parsed.searchParams);
        return parsed.toString();
    } catch (error) {
        const [withoutHash, hash = ''] = rawBase.split('#');
        const [path, existingQuery = ''] = withoutHash.split('?');
        const searchParams = new URLSearchParams(existingQuery);
        upsertParams(searchParams);
        const query = searchParams.toString();
        const hashSuffix = hash ? `#${hash}` : '';
        return `${path}${query ? `?${query}` : ''}${hashSuffix}`;
    }
};

const buildAmdStatusCallbackUrl = (baseUrl, context, payload, params) => {
    const callbackBaseUrl = firstNonEmpty(
        context.AMD_STATUS_CALLBACK_URL,
        payload.amdStatusCallback,
        payload.asyncAmdStatusCallback,
        baseUrl
    );

    if (!callbackBaseUrl) {
        return '';
    }

    return buildUrlWithParams(callbackBaseUrl, params);
};

const getAnsweredByFromCall = async (client, callSid) => {
    if (!callSid) {
        return '';
    }

    try {
        const call = await client.calls(callSid).fetch();
        return firstNonEmpty(call && (call.answeredBy || call.answered_by));
    } catch (error) {
        console.log(`Unable to fetch answeredBy for call ${callSid}: ${error.message}`);
        return '';
    }
};

const getCallStatusFromCall = async (client, callSid) => {
    if (!callSid) {
        return '';
    }

    try {
        const call = await client.calls(callSid).fetch();
        return String(firstNonEmpty(call && (call.status || call.callStatus || call.call_status)) || '').toLowerCase();
    } catch (error) {
        console.log(`Unable to fetch call status for ${callSid}: ${error.message}`);
        return '';
    }
};

const logCallStatusTrace = (enabled, tag, details) => {
    if (!enabled) {
        return;
    }

    try {
        console.log(`[CALLSTATUS_TRACE] ${tag} ${JSON.stringify(details || {})}`);
    } catch (error) {
        console.log(`[CALLSTATUS_TRACE] ${tag} serialization_error=${error.message}`);
    }
};

const buildTeamsPayload = (incidentSummary, event, problemId) => {
    const title = cleanForSpeech(firstNonEmpty(event.title, event.Titulo, event.titulo, event.ProblemTitle, event.problemTitle, event.alertTitle, event['event.name']) || 'Incidente sin título');
    const displayId = cleanForSpeech(firstNonEmpty(event.displayId, event.DisplayID, event.displayID, event.display_id, event.problemDisplayId, event.problemId, event.ProblemID, event['event.id'], problemId) || problemId);
    const status = cleanForSpeech(firstNonEmpty(event.status, event.State, event.state, event.problemStatus, event['event.status']) || 'OPEN');
    const causeName = cleanForSpeech(firstNonEmpty(
        getRootCauseValue(event),
        incidentSummary.causeName,
        incidentSummary.serviceName
    ));
    const problemUrl = cleanForSpeech(firstNonEmpty(event.problemUrl, event.ProblemURL, event.url, event.problemURL));

    return {
        '@type': 'MessageCard',
        '@context': 'http://schema.org/extensions',
        summary: `Dynatrace ${incidentSummary.criticality || incidentSummary.priority} ${title}`,
        themeColor: incidentSummary.priority === 'P1' ? 'FF0000' : 'FFA500',
        title: `Dynatrace ${incidentSummary.criticality || incidentSummary.priority} - ${title}`,
        sections: [
            {
                facts: [
                    { name: 'Incidente', value: displayId },
                    { name: 'Criticidad', value: incidentSummary.criticality || 'N/D' },
                    { name: 'Categoría', value: incidentSummary.category },
                    { name: 'Prioridad', value: incidentSummary.priority },
                    { name: 'Severidad', value: getNormalizedSeverity(event) || 'N/D' },
                    { name: 'Estado', value: status },
                    { name: 'Area', value: incidentSummary.incidentArea || 'N/D' },
                    { name: 'Causa', value: causeName || 'N/D' },
                    { name: 'Entidad afectada', value: incidentSummary.affectedEntity || 'N/D' },
                    { name: 'Zona', value: incidentSummary.managementZone || 'N/D' },
                    { name: 'Monitor/servicio', value: incidentSummary.monitorName || 'N/D' },
                    { name: 'Web request service', value: incidentSummary.webRequestService || 'N/D' },
                    { name: 'Web service', value: incidentSummary.webService || 'N/D' },
                    { name: 'Ubicación afectada', value: incidentSummary.affectedLocation || 'N/D' },
                    { name: 'Llamadas potencialmente afectadas', value: incidentSummary.potentiallyAffectedCalls || 'N/D' },
                    { name: 'URL afectada', value: incidentSummary.affectedUrl || 'N/D' }
                ].filter((fact) => fact.value && fact.value !== 'N/D'),
                text: [
                    incidentSummary.extendedDetails || incidentSummary.compactDetails || incidentSummary.details || incidentSummary.voiceMessage,
                    incidentSummary.affectedUrl ? `URL afectada: ${incidentSummary.affectedUrl}` : ''
                ].filter(Boolean).join('\n\n')
            }
        ],
        potentialAction: problemUrl ? [
            {
                '@type': 'OpenUri',
                name: 'Abrir incidente',
                targets: [
                    { os: 'default', uri: problemUrl }
                ]
            }
        ] : []
    };
};

const normalizeMonitoringUrl = (value) => {
    const raw = String(value || '').trim();
    if (!raw) {
        return '';
    }

    const withProtocol = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    return withProtocol.replace(/\/$/, '');
};

const resolveMonitoringBackendUrls = (context, payload) => {
    const candidates = [
        context.MONITORING_BACKEND_URL,
        context.MONITORING_BACKEND,
        context.DASHBOARD_BACKEND_URL,
        context.INCIDENT_DASHBOARD_URL,
        payload.monitoringBackendUrl,
        payload.monitoring_backend_url,
        payload.dashboardBackendUrl
    ].filter(Boolean);

    const fromCsv = String(context.MONITORING_BACKEND_URLS || payload.monitoringBackendUrls || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);

    const all = [...candidates, ...fromCsv].map((item) => normalizeMonitoringUrl(item)).filter(Boolean);
    return all.filter((item, index) => all.indexOf(item) === index);
};

const resolveAppWebhookUrl = (context, payload) => firstNonEmpty(
    context.APP_WEBHOOK_URL,
    context.MONITORING_APP_WEBHOOK_URL,
    context.DASHBOARD_WEBHOOK_URL,
    payload.appWebhookUrl,
    payload.monitoringAppWebhookUrl,
    payload.dashboardWebhookUrl
);

const parseOncallRoster = (value) => {
    if (!value) {
        return null;
    }

    if (typeof value === 'object' && !Array.isArray(value)) {
        return value;
    }

    try {
        return JSON.parse(String(value));
    } catch (error) {
        throw new Error('Invalid ONCALL_ROSTER JSON. Expected format: {"level1":["+569..."],"level2":["+569..."]}');
    }
};

const resolveSpecialistName = (context, payload, phoneNumber) => {
    if (!phoneNumber) {
        return '';
    }

    const candidates = [
        context.ONCALL_CONTACTS,
        context.ONCALL_PHONEBOOK,
        payload.oncallContacts,
        payload.oncallPhonebook
    ];

    for (const source of candidates) {
        if (!source) {
            continue;
        }

        let parsed = source;
        if (typeof source === 'string') {
            try {
                parsed = JSON.parse(source);
            } catch (error) {
                parsed = null;
            }
        }

        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
            continue;
        }

        const directName = firstNonEmpty(parsed[phoneNumber], parsed[normalizePhoneNumber(phoneNumber)]);
        if (directName) {
            return cleanForSpeech(directName);
        }

        for (const [key, value] of Object.entries(parsed)) {
            if (normalizePhoneNumber(key) === normalizePhoneNumber(phoneNumber)) {
                const normalizedName = firstNonEmpty(value);
                if (normalizedName) {
                    return cleanForSpeech(normalizedName);
                }
            }
        }
    }

    return '';
};

const normalizePhoneNumber = (value) => String(value || '').replace(/[^\d]/g, '');

const asPositiveInt = (value, fallback) => {
    const parsed = parseInt(String(value || ''), 10);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};

const buildAckActionUrl = ({ functionBaseUrl, problemId, level, attempt, levelIndex, toNumber, replayCount }) => {
    return buildUrlWithParams(functionBaseUrl, {
        mode: 'ack',
        problemId,
        level,
        attempt,
        levelIndex,
        to: toNumber || '',
        replayCount
    });
};

const buildStatusCallbackUrl = ({ functionBaseUrl, problemId, level, attempt, levelIndex, ackConfirmed }) => {
    return buildUrlWithParams(functionBaseUrl, {
        mode: 'callback',
        problemId,
        level,
        attempt,
        levelIndex,
        ackConfirmed: ackConfirmed ? '1' : '0'
    });
};

const persistAckConfirmationOnCall = async ({ client, callSid, functionBaseUrl, problemId, level, attempt, levelIndex }) => {
    if (!client || !callSid || !functionBaseUrl) {
        return false;
    }

    const confirmedStatusCallbackUrl = buildStatusCallbackUrl({
        functionBaseUrl,
        problemId,
        level,
        attempt,
        levelIndex,
        ackConfirmed: true
    });

    try {
        await client.calls(callSid).update({
            statusCallback: confirmedStatusCallbackUrl,
            statusCallbackMethod: 'POST'
        });
        return true;
    } catch (error) {
        console.log(`Unable to persist ACK confirmation in status callback for call ${callSid}: ${error.message}`);
        return false;
    }
};

const buildInteractiveCallTwiml = ({ sayMessage, actionUrl, voice, language }) => {
    const prompt = `${cleanForSpeech(sayMessage)} Para confirmar la recepción del mensaje, marque 1. Para escuchar el mensaje nuevamente, marque 2.`;

    return `<Response><Gather input="dtmf" numDigits="1" timeout="8" actionOnEmptyResult="true" action="${xmlEscape(actionUrl)}" method="POST"><Say voice="${xmlEscape(voice)}" language="${xmlEscape(language)}">${xmlEscape(prompt)}</Say></Gather></Response>`;
};

const buildSimpleTwiml = ({ message, voice, language }) => {
    return `<Response><Say voice="${xmlEscape(voice)}" language="${xmlEscape(language)}">${xmlEscape(message)}</Say><Hangup/></Response>`;
};

const buildXmlCallbackResponse = (xmlBody) => {
    const response = new Twilio.Response();
    response.appendHeader('Content-Type', 'text/xml');
    response.setBody(xmlBody);
    return response;
};

const fetchWithTimeoutAndRetry = async (url, requestOptions, timeoutMs = 7000, retries = 1) => {
    let lastError = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(url, {
                ...requestOptions,
                signal: controller.signal
            });
            clearTimeout(timeout);
            return response;
        } catch (error) {
            clearTimeout(timeout);
            lastError = error;
            if (attempt === retries) {
                throw lastError;
            }
        }
    }

    throw lastError || new Error('Request failed');
};

const postIncidentToApp = async ({
    appWebhookUrl,
    appWebhookSecret,
    incidentSummary,
    payload,
    problemId,
    calledNumber,
    calledPersonName,
    callStatus,
    phoneSent,
    smsSent,
    teamsSent,
    timeoutMs = 7000,
    retries = 1
}) => {
    if (!appWebhookUrl) {
        return { delivered: false, reason: 'missing_app_webhook_url' };
    }

    const resolvedCauseName = firstNonEmpty(
        incidentSummary.causeName,
        getRootCauseValue(payload),
        extractDisplayValue(payload.cause_name),
        extractDisplayValue(payload.causeName),
        extractDisplayValue(payload.ProblemRootCause),
        extractDisplayValue(payload.problemRootCause),
        getPayloadValueByLooseKey(payload, 'ProblemRootCause', 'problemRootCause', 'problem_root_cause', 'problemrootcause')
    );

    const resolvedAffectedEntity = firstNonEmpty(
        incidentSummary.affectedEntity,
        getAffectedEntityValue(payload),
        extractDisplayValue(payload.affected_entity),
        extractDisplayValue(payload.affectedEntity),
        extractDisplayValue(payload.ProblemImpact),
        extractDisplayValue(payload.problemImpact),
        getPayloadValueByLooseKey(payload, 'ProblemImpact', 'problemImpact', 'problem_impact', 'problemimpact')
    );

    const resolvedIncidentArea = firstNonEmpty(
        incidentSummary.incidentArea,
        getResolvedIncidentArea(payload),
        extractDisplayValue(payload.incident_area),
        extractDisplayValue(payload.incidentArea),
        extractDisplayValue(payload.area),
        extractDisplayValue(payload.area_name),
        extractDisplayValue(payload.areaName),
        extractDisplayValue(payload.area_code),
        extractDisplayValue(payload.areaCode)
    );

    const body = {
        incident_title: incidentSummary.title,
        incident_status: firstNonEmpty(callStatus, payload.status, payload.State, payload.state, payload.problemStatus, payload['event.status'], 'OPEN'),
        incident_severity: firstNonEmpty(getNormalizedSeverity(payload), incidentSummary.priority, 'UNKNOWN'),
        incident_description: firstNonEmpty(incidentSummary.extendedDetails, incidentSummary.compactDetails, incidentSummary.details),
        called_number: calledNumber,
        called_person_name: calledPersonName || '',
        problem_id: problemId,
        incident_attended: false,
        priority: incidentSummary.priority,
        category: incidentSummary.category,
        criticality: incidentSummary.criticality,
        cause_name: resolvedCauseName,
        affected_entity: resolvedAffectedEntity,
        incident_area: resolvedIncidentArea,
        source: 'twilio-function',
        channels: {
            phone: Boolean(phoneSent),
            sms: Boolean(smsSent),
            teams: Boolean(teamsSent)
        }
    };

    console.log('Resolved incident fields for app webhook:', JSON.stringify({
        problem_id: problemId,
        cause_name: body.cause_name || null,
        affected_entity: body.affected_entity || null,
        incident_area: body.incident_area || null
    }));

    const response = await fetchWithTimeoutAndRetry(appWebhookUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            ...(appWebhookSecret ? { 'X-Webhook-Secret': appWebhookSecret } : {})
        },
        body: JSON.stringify(body)
    }, timeoutMs, retries);

    if (!response.ok) {
        const responseBody = await response.text();
        throw new Error(`App webhook failed with status ${response.status}: ${responseBody}`);
    }

    let parsedBody = null;
    try {
        parsedBody = await response.json();
    } catch (error) {
        parsedBody = null;
    }

    return {
        delivered: true,
        statusCode: response.status,
        created: parsedBody && typeof parsedBody.created === 'boolean' ? parsedBody.created : response.status === 201,
        duplicate: Boolean(parsedBody && parsedBody.duplicate),
        reason: firstNonEmpty(parsedBody && parsedBody.reason)
    };
};

const postIncidentAckToApp = async ({
    appWebhookUrl,
    appWebhookSecret,
    problemId,
    calledNumber,
    incidentAttended,
    incidentStatus
}) => {
    if (!appWebhookUrl || !problemId) {
        return { delivered: false, reason: !appWebhookUrl ? 'missing_app_webhook_url' : 'missing_problem_id' };
    }

    const body = {
        event_type: 'ack_update',
        problem_id: problemId,
        called_number: calledNumber || '',
        incident_attended: Boolean(incidentAttended),
        incident_status: firstNonEmpty(incidentStatus, incidentAttended ? 'ACKNOWLEDGED' : 'NOT_ACKNOWLEDGED')
    };

    const response = await fetchWithTimeoutAndRetry(appWebhookUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            ...(appWebhookSecret ? { 'X-Webhook-Secret': appWebhookSecret } : {})
        },
        body: JSON.stringify(body)
    }, 7000, 1);

    if (!response.ok) {
        const responseBody = await response.text();
        throw new Error(`App webhook ack update failed with status ${response.status}: ${responseBody}`);
    }

    return { delivered: true };
};

const reserveEscalationToApp = async ({
    appWebhookUrl,
    appWebhookSecret,
    problemId,
    calledNumber,
    level,
    reason
}) => {
    if (!appWebhookUrl) {
        return { delivered: false, reserved: false, reason: 'missing_app_webhook_url' };
    }

    if (!problemId || !calledNumber) {
        return { delivered: false, reserved: false, reason: !problemId ? 'missing_problem_id' : 'missing_called_number' };
    }

    const body = {
        event_type: 'escalation_reserve',
        problem_id: problemId,
        called_number: calledNumber,
        level: String(level || '2'),
        reserve_reason: firstNonEmpty(reason, 'escalation')
    };

    const response = await fetchWithTimeoutAndRetry(appWebhookUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            ...(appWebhookSecret ? { 'X-Webhook-Secret': appWebhookSecret } : {})
        },
        body: JSON.stringify(body)
    }, 7000, 1);

    if (!response.ok) {
        const responseBody = await response.text();
        throw new Error(`App webhook escalation reserve failed with status ${response.status}: ${responseBody}`);
    }

    let parsedBody = null;
    try {
        parsedBody = await response.json();
    } catch (error) {
        parsedBody = null;
    }

    return {
        delivered: true,
        reserved: Boolean(parsedBody && parsedBody.reserved),
        duplicate: Boolean(parsedBody && parsedBody.duplicate),
        reason: firstNonEmpty(parsedBody && parsedBody.reason)
    };
};

const postMonitoringEvent = async ({ monitoringBackendUrls, ingestToken, eventType, problemId, payload }) => {
    if (!Array.isArray(monitoringBackendUrls) || !monitoringBackendUrls.length) {
        return;
    }

    for (const url of monitoringBackendUrls) {
        try {
            const response = await fetch(`${url}/api/ingest`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...(ingestToken ? { 'X-Ingest-Token': ingestToken } : {})
                },
                body: JSON.stringify({
                    eventType,
                    source: 'twilio-function',
                    problemId,
                    timestamp: new Date().toISOString(),
                    ...(payload || {})
                })
            });

            if (response.ok) {
                return;
            }

            const body = await response.text();
            console.log(`Monitoring backend ${url} rejected event ${eventType} with status ${response.status}: ${body}`);
        } catch (error) {
            console.log(`Monitoring backend ${url} event ${eventType} failed: ${error.message}`);
        }
    }
};

// ─── Retry Scheduling Helpers (Twilio Sync) ────────────────────────────────

const scheduleRetry = async ({ client, syncServiceSid, problemId, level, attempt, toNumber, delayMinutes }) => {
    if (!syncServiceSid) {
        console.log('[RETRY] SYNC_SERVICE_SID not configured, skipping retry schedule');
        return false;
    }
    const retryAt = Date.now() + (delayMinutes * 60 * 1000);
    const itemKey = `retry:${problemId}:L${level}:A${attempt + 1}`;
    try {
        await client.sync.v1.services(syncServiceSid)
            .syncMaps('pending_retries')
            .syncMapItems
            .create({
                key: itemKey,
                data: {
                    problemId,
                    level: String(level),
                    attempt: attempt + 1,
                    toNumber,
                    retryAt,
                    scheduledAt: Date.now(),
                    status: 'pending'
                },
                ttl: 86400
            });
        console.log(`[RETRY] Scheduled retry: ${itemKey} at ${new Date(retryAt).toISOString()}`);
        return true;
    } catch (err) {
        if (err.code === 54208) {
            console.log(`[RETRY] Retry already exists for ${itemKey}, skipping`);
            return false;
        }
        console.log(`[RETRY] Failed to schedule retry: ${err.message}`);
        return false;
    }
};

const getDuePendingRetries = async ({ client, syncServiceSid }) => {
    if (!syncServiceSid) return [];
    try {
        const items = await client.sync.v1.services(syncServiceSid)
            .syncMaps('pending_retries')
            .syncMapItems
            .list({ limit: 50 });
        const now = Date.now();
        return items.filter(item => item.data && item.data.status === 'pending' && item.data.retryAt <= now + 60000);
    } catch (err) {
        console.log(`[RETRY] Failed to read pending retries: ${err.message}`);
        return [];
    }
};

// ────────────────────────────────────────────────────────────────────────────

exports.handler = async function (context, event, callback) {
    let payload = buildIncomingPayload(event);
    payload = {
        ...payload,
        __areaTagGroups: resolveAreaTagGroupsFromContext(context, payload)
    };
    let enrichedProblemDetails = null;
    const client = require('twilio')(context.ACCOUNT_SID, context.AUTH_TOKEN);
    const debugEnabled = isDebugEnabled(context.DEBUG_DYNATRACE_PAYLOAD || payload.debugDynatracePayload);

    const fromNumber = context.TWILIO_FROM || payload.from;
    const smsFromNumber = context.TWILIO_SMS_FROM || fromNumber;
    const functionBaseUrl = context.FUNCTION_BASE_URL || payload.functionBaseUrl;
    const teamsWebhookUrl = buildTeamsWebhookUrl(context, payload);
    const appWebhookUrl = resolveAppWebhookUrl(context, payload);
    const appWebhookSecret = firstNonEmpty(
        context.APP_WEBHOOK_SECRET,
        context.MONITORING_APP_WEBHOOK_SECRET,
        payload.appWebhookSecret,
        payload.monitoringAppWebhookSecret
    );
    const monitoringBackendUrls = resolveMonitoringBackendUrls(context, payload);
    const monitoringIngestToken = firstNonEmpty(
        context.MONITORING_INGEST_TOKEN,
        context.INGEST_TOKEN,
        payload.monitoringIngestToken,
        payload.ingestToken
    );

    const problemId = resolveProblemId(payload);
    const mode = firstNonEmpty(event.mode, payload.mode, 'start').toLowerCase();
    const hasCallbackCallSid = Boolean(firstNonEmpty(event.CallSid, payload.CallSid, payload.callSid));
    const hasExplicitMode = Boolean(firstNonEmpty(payload.mode, event.mode));
    const level = payload.level || '1';
    const attempt = parseInt(payload.attempt || '1', 10);
    const levelIndex = parseInt(payload.levelIndex || '0', 10);
    const requireDtmfAck = ['1', 'true', 'yes', 'on'].includes(firstNonEmpty(context.REQUIRE_DTMF_ACK, payload.requireDtmfAck, 'true').toLowerCase());
    const traceCallStatus = asBooleanFlag(firstNonEmpty(context.TRACE_CALL_STATUS, payload.traceCallStatus, 'true'), true);
    const strictIdempotency = asBooleanFlag(firstNonEmpty(context.STRICT_IDEMPOTENCY, payload.strictIdempotency, 'true'), true);
    const disableSmokeEscalation = asBooleanFlag(firstNonEmpty(context.DISABLE_SMOKE_ESCALATION, payload.disableSmokeEscalation, 'true'), true);
    const enforceSingleDialPerLevel = asBooleanFlag(firstNonEmpty(context.ENFORCE_SINGLE_DIAL_PER_LEVEL, payload.enforceSingleDialPerLevel, 'true'), true);
    const recentDialGuardEnabled = asBooleanFlag(firstNonEmpty(context.ENABLE_RECENT_DIAL_GUARD, payload.enableRecentDialGuard, 'true'), true);
    const enableActiveDialGuard = asBooleanFlag(firstNonEmpty(context.ENABLE_ACTIVE_DIAL_GUARD, payload.enableActiveDialGuard, 'true'), true);
    const enableSmokeStartRecentGuard = asBooleanFlag(firstNonEmpty(context.ENABLE_SMOKE_START_RECENT_GUARD, payload.enableSmokeStartRecentGuard, 'true'), true);
    const smokeStartRecentGuardWindowSeconds = Math.max(5, asPositiveInt(firstNonEmpty(context.SMOKE_START_RECENT_GUARD_WINDOW_SECONDS, payload.smokeStartRecentGuardWindowSeconds, '45'), 45));
    const enableSmokeEscalationRecentGuard = asBooleanFlag(firstNonEmpty(context.ENABLE_SMOKE_ESCALATION_RECENT_GUARD, payload.enableSmokeEscalationRecentGuard, 'true'), true);
    const smokeEscalationRecentGuardWindowSeconds = Math.max(5, asPositiveInt(firstNonEmpty(context.SMOKE_ESCALATION_RECENT_GUARD_WINDOW_SECONDS, payload.smokeEscalationRecentGuardWindowSeconds, '120'), 120));
    const recentDialGuardWindowSeconds = Math.max(1, asPositiveInt(firstNonEmpty(context.RECENT_DIAL_GUARD_WINDOW_SECONDS, payload.recentDialGuardWindowSeconds, '90'), 90));
    const recentDialGuardMaxResults = Math.max(1, asPositiveInt(firstNonEmpty(context.RECENT_DIAL_GUARD_MAX_RESULTS, payload.recentDialGuardMaxResults, '100'), 100));
    const singleDialPerLevelWindowSeconds = Math.max(
        recentDialGuardWindowSeconds,
        Math.max(1, asPositiveInt(firstNonEmpty(context.SINGLE_DIAL_PER_LEVEL_WINDOW_SECONDS, payload.singleDialPerLevelWindowSeconds, '1800'), 1800))
    );
    const configuredSingleDialSmokeWindowRaw = firstNonEmpty(
        context.SINGLE_DIAL_SMOKE_WINDOW_SECONDS,
        payload.singleDialSmokeWindowSeconds
    );
    const hasExplicitSingleDialSmokeWindow = Boolean(configuredSingleDialSmokeWindowRaw);
    const singleDialSmokeWindowSeconds = hasExplicitSingleDialSmokeWindow
        ? Math.max(5, asPositiveInt(configuredSingleDialSmokeWindowRaw, singleDialPerLevelWindowSeconds))
        : singleDialPerLevelWindowSeconds;
    const flowLockTtlSeconds = asPositiveInt(firstNonEmpty(context.FLOW_LOCK_TTL_SECONDS, payload.flowLockTtlSeconds, '1800'), 1800);
    const flowLockTtlMs = Math.max(1000, flowLockTtlSeconds * 1000);
    const ackMaxReplays = asPositiveInt(firstNonEmpty(context.ACK_MAX_REPLAYS, payload.ackMaxReplays, '0'), 0);
    const callMessageVoice = firstNonEmpty(context.TWILIO_MESSAGE_VOICE, context.TWILIO_VOICE, payload.messageVoice, 'Polly.Lupe-Generative');
    const callMessageLanguage = firstNonEmpty(context.TWILIO_MESSAGE_LANGUAGE, context.TWILIO_LANGUAGE, payload.messageLanguage, 'es-MX');
    const amdStatusCallbackBaseUrl = firstNonEmpty(
        context.AMD_STATUS_CALLBACK_URL,
        payload.amdStatusCallback,
        payload.asyncAmdStatusCallback,
        functionBaseUrl
    );

    let roster = null;
    try {
        roster = parseOncallRoster(context.ONCALL_ROSTER);
    } catch (error) {
        console.log(error.message);
        return callback(error);
    }

    const eventLevel1List = normalizeList(payload.level1);
    const eventLevel2List = normalizeList(payload.level2);
    const rosterLevel1List = normalizeList(roster && roster.level1);
    const rosterLevel2List = normalizeList(roster && roster.level2);

    const level1List = eventLevel1List.length ? eventLevel1List : rosterLevel1List;
    const level2List = eventLevel2List.length ? eventLevel2List : rosterLevel2List;
    const skipDynatraceLookup = shouldSkipDynatraceLookupForProblemId(context, payload, problemId);
    const dynatraceIdentitySignalsPresent = hasDynatraceIdentitySignals(payload);
    const isSmokeLikeProblemId = shouldSkipDynatraceLookupForProblemId(context, payload, problemId);
    const enforceSmokeSingleCall = asBooleanFlag(firstNonEmpty(
        context.ENFORCE_SMOKE_SINGLE_CALL,
        payload.enforceSmokeSingleCall,
        'true'
    ), true);
    const smokeSingleCallWindowSeconds = Math.min(600, Math.max(60, asPositiveInt(firstNonEmpty(
        context.SMOKE_SINGLE_CALL_WINDOW_SECONDS,
        payload.smokeSingleCallWindowSeconds,
        '300'
    ), 300)));
    const effectiveDisableSmokeEscalation = isSmokeLikeProblemId;
    const enforceSingleDialPerLevelForSmoke = asBooleanFlag(firstNonEmpty(
        context.ENFORCE_SINGLE_DIAL_PER_LEVEL_FOR_SMOKE,
        payload.enforceSingleDialPerLevelForSmoke,
        'false'
    ), false);
    const enableRecentDialGuardForSmoke = asBooleanFlag(firstNonEmpty(
        context.ENABLE_RECENT_DIAL_GUARD_FOR_SMOKE,
        payload.enableRecentDialGuardForSmoke,
        'false'
    ), false);
    const effectiveEnforceSingleDialPerLevel = enforceSingleDialPerLevel && (!isSmokeLikeProblemId || enforceSingleDialPerLevelForSmoke);
    const effectiveRecentDialGuardEnabled = recentDialGuardEnabled && (!isSmokeLikeProblemId || enableRecentDialGuardForSmoke);
    const enforceRecentWindowGuard = effectiveEnforceSingleDialPerLevel || effectiveRecentDialGuardEnabled;
    const smokeStartRecentGuardActive = isSmokeLikeProblemId && enableSmokeStartRecentGuard && !enforceRecentWindowGuard;
    const smokeEscalationRecentGuardActive = isSmokeLikeProblemId && enableSmokeEscalationRecentGuard && !enforceRecentWindowGuard;
    const allowSmokeDialGuardBypass = asBooleanFlag(firstNonEmpty(context.ALLOW_SMOKE_DIAL_GUARD_BYPASS, 'false'), false);
    const requestDialGuardBypass = asBooleanFlag(firstNonEmpty(
        payload.bypassDialGuard,
        payload.forceDialGuardBypass,
        payload.smokeBypassDialGuard,
        event.bypassDialGuard,
        event.forceDialGuardBypass,
        'false'
    ), false);
    const dialGuardBypassActive = allowSmokeDialGuardBypass && requestDialGuardBypass;
    const smokeWindowApplied = isSmokeLikeProblemId && hasExplicitSingleDialSmokeWindow;
    const effectiveSingleDialWindowSeconds = smokeWindowApplied
        ? Math.min(singleDialPerLevelWindowSeconds, singleDialSmokeWindowSeconds)
        : singleDialPerLevelWindowSeconds;

    logCallStatusTrace(traceCallStatus, 'single_dial_window', {
        problemId,
        isSmokeLikeProblemId,
        hasExplicitSingleDialSmokeWindow,
        smokeWindowApplied,
        enableActiveDialGuard,
        enforceSingleDialPerLevelForSmoke,
        enableRecentDialGuardForSmoke,
        effectiveEnforceSingleDialPerLevel,
        effectiveRecentDialGuardEnabled,
        enforceRecentWindowGuard,
        enableSmokeStartRecentGuard,
        smokeStartRecentGuardWindowSeconds,
        smokeStartRecentGuardActive,
        enableSmokeEscalationRecentGuard,
        smokeEscalationRecentGuardWindowSeconds,
        smokeEscalationRecentGuardActive,
        disableSmokeEscalation,
        effectiveDisableSmokeEscalation,
        enforceSmokeSingleCall,
        smokeSingleCallWindowSeconds,
        allowSmokeDialGuardBypass,
        requestDialGuardBypass,
        dialGuardBypassActive,
        baseSingleDialPerLevelWindowSeconds: singleDialPerLevelWindowSeconds,
        smokeSingleDialWindowSeconds: singleDialSmokeWindowSeconds,
        effectiveSingleDialWindowSeconds
    });

    if (
        mode === 'start'
        && !hasCallbackCallSid
        && shouldEnrichFromDynatrace(context, payload)
        && requiresDynatraceEnrichment(payload)
        && problemId !== 'unknown'
        && !isSyntheticProblemId(problemId)
        && !skipDynatraceLookup
        && dynatraceIdentitySignalsPresent
    ) {
        try {
            enrichedProblemDetails = await fetchDynatraceProblemDetails(context, payload, problemId);
            payload = enrichPayloadWithDynatraceProblem(payload, enrichedProblemDetails, context);
        } catch (error) {
            console.log(`Dynatrace enrichment failed for ${problemId}: ${error.message}`);
        }
    }

    const incidentSummary = buildIncidentSummary(payload, problemId);
    const message = payload.message || incidentSummary.voiceMessage;
    const level2Message = payload.level2Message || `Escalamiento automático. ${incidentSummary.voiceMessage}`;
    const smsMessage = payload.smsMessage || incidentSummary.smsMessage;
    const startLockKey = `start:${problemId}`;
    const allowedStatusForNotification = shouldNotifyForStatus(context, payload);

    if (!monitoringBackendUrls.length) {
        console.log('MONITORING_BACKEND_URL not configured; dashboard ingestion is disabled');
    }

    if (debugEnabled) {
        console.log('Dynatrace raw event:', JSON.stringify(event));
        console.log('Dynatrace merged payload:', JSON.stringify(payload));
        console.log('Dynatrace payload keys:', JSON.stringify(getObjectKeys(payload)));
        console.log('Dynatrace entity tags:', JSON.stringify(payload.entityTags || payload.EntityTags || payload.tags || payload.entity_tags || payload['Entity tags'] || []));
        console.log('Dynatrace observability tag detected:', hasObservabilityCriticalTag(payload));
        console.log('Dynatrace area tag groups:', JSON.stringify(getAreaTagGroups(payload)));
        console.log('Dynatrace area matches:', JSON.stringify(getAreaTagMatches(payload)));
        console.log('Dynatrace resolved area:', getResolvedIncidentArea(payload) || '[empty]');
        console.log('Teams webhook configured:', Boolean(teamsWebhookUrl));
        console.log('Teams webhook length:', teamsWebhookUrl ? teamsWebhookUrl.length : 0);
        console.log('Dynatrace resolved cause:', getRootCauseValue(payload) || '[empty]');
        console.log('Dynatrace resolved affected entity:', getAffectedEntityValue(payload) || '[empty]');
        console.log('Dynatrace compact details:', incidentSummary.compactDetails || '[empty]');
        if (enrichedProblemDetails) {
            console.log('Dynatrace problem details keys:', JSON.stringify(getObjectKeys(enrichedProblemDetails)));
            console.log('Dynatrace problem details rootCause:', JSON.stringify(enrichedProblemDetails.rootCause || null));
            console.log('Dynatrace problem details rootCauseEntity:', JSON.stringify(enrichedProblemDetails.rootCauseEntity || null));
            console.log('Dynatrace problem details affectedEntities:', JSON.stringify(enrichedProblemDetails.affectedEntities || []));
            console.log('Dynatrace problem details impactedEntities:', JSON.stringify(enrichedProblemDetails.impactedEntities || []));
        }
    }

    console.log('Dynatrace alert parsed:', JSON.stringify({
        problemId,
        title: incidentSummary.title,
        criticality: incidentSummary.criticality,
        severity: getNormalizedSeverity(payload),
        priority: incidentSummary.priority,
        causeName: incidentSummary.causeName || incidentSummary.serviceName,
        affectedEntity: incidentSummary.affectedEntity,
        incidentArea: incidentSummary.incidentArea || ''
    }));

    if (!fromNumber || !functionBaseUrl) {
        const missingConfig = [];
        if (!fromNumber) missingConfig.push('TWILIO_FROM');
        if (!functionBaseUrl) missingConfig.push('FUNCTION_BASE_URL');
        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'config_error',
            problemId,
            payload: {
                callStatus: 'failed',
                missingConfig: missingConfig.join(','),
                severity: getNormalizedSeverity(payload),
                title: incidentSummary.title
            }
        });
        return callback(new Error('Missing TWILIO_FROM or FUNCTION_BASE_URL'));
    }

    const dial = async (toNumber, nextLevel, nextAttempt, nextIndex, options = {}) => {
        const smsSent = Boolean(options.smsSent);
        const teamsSent = Boolean(options.teamsSent);
        const targetProblemId = options.targetProblemId || problemId;
        const isEscalationDial = String(nextLevel) !== '1';
        const includeRecentLookupForDial = enforceRecentWindowGuard || (smokeEscalationRecentGuardActive && isEscalationDial);
        const dialGuardWindowSeconds = includeRecentLookupForDial
            ? (
                effectiveEnforceSingleDialPerLevel
                    ? effectiveSingleDialWindowSeconds
                    : (smokeEscalationRecentGuardActive && isEscalationDial && !enforceRecentWindowGuard
                        ? smokeEscalationRecentGuardWindowSeconds
                        : recentDialGuardWindowSeconds)
            )
            : recentDialGuardWindowSeconds;
        const shouldRunDialGuard = (enableActiveDialGuard || includeRecentLookupForDial) && !dialGuardBypassActive && !options.bypassGuard;
        const sayMessage = nextLevel === '2' ? level2Message : message;

        if (dialGuardBypassActive) {
            logCallStatusTrace(traceCallStatus, 'dial_guard_bypass', {
                problemId,
                toNumber,
                fromNumber,
                level: nextLevel,
                attempt: nextAttempt,
                levelIndex: nextIndex,
                reason: 'smoke_test_bypass_active'
            });
        }

        if (shouldRunDialGuard) {
            const duplicateDial = await hasExistingOutboundDial({
                client,
                toNumber,
                fromNumber,
                windowSeconds: dialGuardWindowSeconds,
                maxResults: recentDialGuardMaxResults,
                includeRecentLookup: includeRecentLookupForDial
            });

            logCallStatusTrace(traceCallStatus, 'dial_guard_probe', {
                problemId: targetProblemId,
                level: nextLevel,
                attempt: nextAttempt,
                levelIndex: nextIndex,
                toNumber,
                fromNumber,
                matched: Boolean(duplicateDial && duplicateDial.matched),
                reason: firstNonEmpty(duplicateDial && duplicateDial.reason),
                source: firstNonEmpty(duplicateDial && duplicateDial.source),
                hint: firstNonEmpty(duplicateDial && duplicateDial.hint),
                matchedCallSid: firstNonEmpty(duplicateDial && duplicateDial.matchedCall && duplicateDial.matchedCall.sid),
                matchedCallStatus: firstNonEmpty(duplicateDial && duplicateDial.matchedCall && duplicateDial.matchedCall.status),
                matchedCallAgeSeconds: duplicateDial && duplicateDial.matchedCall ? duplicateDial.matchedCall.ageSeconds : null,
                recentLookupEnabled: includeRecentLookupForDial,
                guardWindowSeconds: includeRecentLookupForDial ? dialGuardWindowSeconds : 0,
                activeProbeReason: firstNonEmpty(duplicateDial && duplicateDial.diagnostics && duplicateDial.diagnostics.activeReason),
                recentProbeReason: firstNonEmpty(duplicateDial && duplicateDial.diagnostics && duplicateDial.diagnostics.recentReason)
            });

            if (duplicateDial.matched) {
                const dialGuardError = new Error(`duplicate dial blocked (${duplicateDial.reason || 'guard_match'})`);
                dialGuardError.code = 'DUPLICATE_DIAL_BLOCKED';
                dialGuardError.duplicateDialReason = firstNonEmpty(duplicateDial.reason, 'guard_match');
                dialGuardError.duplicateDialHint = firstNonEmpty(duplicateDial.hint);
                dialGuardError.duplicateDialSource = firstNonEmpty(duplicateDial.source);
                dialGuardError.duplicateDialMatchedCall = duplicateDial.matchedCall || null;
                dialGuardError.duplicateDialDiagnostics = duplicateDial.diagnostics || null;
                dialGuardError.duplicateDialWindowSeconds = includeRecentLookupForDial ? dialGuardWindowSeconds : 0;
                throw dialGuardError;
            }
        }

        const amdStatusCallback = buildAmdStatusCallbackUrl(
            amdStatusCallbackBaseUrl,
            context,
            payload,
            {
                mode: 'amd',
                problemId: targetProblemId,
                level: nextLevel,
                attempt: nextAttempt,
                levelIndex: nextIndex,
                to: toNumber
            }
        );
        const ackActionUrl = buildAckActionUrl({
            functionBaseUrl,
            problemId: targetProblemId,
            level: nextLevel,
            attempt: nextAttempt,
            levelIndex: nextIndex,
            toNumber,
            replayCount: 0
        });
        const statusCallbackUrl = buildStatusCallbackUrl({
            functionBaseUrl,
            problemId: targetProblemId,
            level: nextLevel,
            attempt: nextAttempt,
            levelIndex: nextIndex,
            ackConfirmed: false
        });
        const callTwiml = requireDtmfAck
            ? buildInteractiveCallTwiml({
                sayMessage,
                actionUrl: ackActionUrl,
                voice: callMessageVoice,
                language: callMessageLanguage
            })
            : buildSimpleTwiml({
                message: sayMessage,
                voice: callMessageVoice,
                language: callMessageLanguage
            });

        console.log(`Dialing level ${nextLevel} attempt ${nextAttempt} to ${toNumber} (index ${nextIndex})`);
        const call = await client.calls.create({
            to: toNumber,
            from: fromNumber,
            twiml: callTwiml,
            machineDetection: 'Enable',
            asyncAmd: true,
            ...(amdStatusCallback ? {
                asyncAmdStatusCallback: amdStatusCallback,
                asyncAmdStatusCallbackMethod: 'POST'
            } : {}),
            statusCallback: statusCallbackUrl,
            statusCallbackMethod: 'POST',
            statusCallbackEvent: ['completed', 'no-answer', 'busy', 'failed', 'canceled']
        });

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'call_initiated',
            problemId: targetProblemId,
            payload: {
                callSid: call.sid,
                specialistPhone: toNumber,
                callStatus: 'initiated',
                answered: false,
                smsSent,
                teamsSent,
                level: nextLevel,
                attempt: nextAttempt,
                levelIndex: nextIndex
            }
        });

        return { sent: true, call };
    };

    const sendSms = async (toNumber, nextLevel, nextAttempt, callStatus) => {
        if (!smsFromNumber) {
            console.log('Missing TWILIO_SMS_FROM; SMS not sent');
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'sms_skipped',
                problemId,
                payload: {
                    specialistPhone: toNumber,
                    smsSent: false,
                    callStatus,
                    reason: 'missing_twilio_sms_from'
                }
            });
            return false;
        }
        const smsMessageWithLevel = `${smsMessage}\n[L${nextLevel}A${nextAttempt}]`;
        const safeSmsMessage = truncateText(smsMessageWithLevel, 150);
        console.log(`Sending SMS to ${toNumber} for level ${nextLevel} attempt ${nextAttempt} (status ${callStatus}) with ${safeSmsMessage.length} chars`);
        const result = await client.messages.create({
            to: toNumber,
            from: smsFromNumber,
            body: safeSmsMessage
        });
        console.log(`SMS queued with SID ${result.sid}`);

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'sms_sent',
            problemId,
            payload: {
                specialistPhone: toNumber,
                smsSent: true,
                callStatus
            }
        });

        return true;
    };

    const trySendSms = async (toNumber, nextLevel, nextAttempt, callStatus) => {
        try {
            return await sendSms(toNumber, nextLevel, nextAttempt, callStatus);
        } catch (error) {
            console.log(`SMS send error (non-blocking): ${error.message}`);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'sms_failed',
                problemId,
                payload: {
                    specialistPhone: toNumber,
                    smsSent: false,
                    callStatus: 'failed',
                    error: truncateText(error.message || 'unknown error', 260)
                }
            });
            return false;
        }
    };

    const tryDial = async (toNumber, nextLevel, nextAttempt, nextIndex, targetProblemId = problemId, bypassGuard = false) => {
        if (smsFromNumber && targetProblemId && targetProblemId !== 'unknown') {
            try {
                const recentSms = await client.messages.list({ to: toNumber, limit: 10 });
                const alreadyProcessed = recentSms.some(msg => 
                    msg.body && msg.body.includes(`ID: ${targetProblemId}`) && msg.body.includes(`[L${nextLevel}A${nextAttempt}]`)
                );
                if (alreadyProcessed) {
                    console.log(`[DEDUPLICATOR] Call to level ${nextLevel} for ${targetProblemId} aborted: SMS already sent!`);
                    return { sent: false, duplicateBlocked: true, reason: 'sms_deduplication_lock_matched' };
                }
            } catch (err) {
                console.log(`[DEDUPLICATOR] SMS deduplication check failed: ${err.message}`);
            }
        }

        try {
            await dial(toNumber, nextLevel, nextAttempt, nextIndex, {
                targetProblemId,
                smsSent: false,
                teamsSent: false,
                bypassGuard
            });
            return {
                sent: true,
                duplicateBlocked: false
            };
        } catch (error) {
            if (error && error.code === 'DUPLICATE_DIAL_BLOCKED') {
                const duplicateDialReason = firstNonEmpty(error.duplicateDialReason, 'duplicate_dial_guard_match');
                const duplicateDialHint = firstNonEmpty(
                    error.duplicateDialHint,
                    buildDuplicateGuardHint({
                        reason: duplicateDialReason,
                        toNumber,
                        fromNumber,
                        windowSeconds: asPositiveInt(error && error.duplicateDialWindowSeconds, effectiveEnforceSingleDialPerLevel ? effectiveSingleDialWindowSeconds : recentDialGuardWindowSeconds),
                        maxResults: recentDialGuardMaxResults
                    })
                );
                const duplicateDialSource = firstNonEmpty(error.duplicateDialSource, 'unknown');
                const duplicateDialMatchedCall = error.duplicateDialMatchedCall || null;
                const duplicateDialDiagnostics = error.duplicateDialDiagnostics || null;
                console.log(`Dial blocked by duplicate guard: ${duplicateDialReason}. Hint: ${duplicateDialHint}`);
                console.log(`[DIAL_GUARD_DIAG] ${JSON.stringify({
                    problemId: targetProblemId,
                    toNumber,
                    fromNumber,
                    level: nextLevel,
                    attempt: nextAttempt,
                    levelIndex: nextIndex,
                    reason: duplicateDialReason,
                    source: duplicateDialSource,
                    hint: duplicateDialHint,
                    matchedCall: duplicateDialMatchedCall,
                    diagnostics: duplicateDialDiagnostics
                })}`);

                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'call_duplicate_blocked_pre_dial',
                    problemId: targetProblemId,
                    payload: {
                        specialistPhone: toNumber,
                        callStatus: 'blocked',
                        level: nextLevel,
                        attempt: nextAttempt,
                        levelIndex: nextIndex,
                        reason: duplicateDialReason,
                        hint: duplicateDialHint,
                        guardSource: duplicateDialSource,
                        matchedCallSid: firstNonEmpty(duplicateDialMatchedCall && duplicateDialMatchedCall.sid),
                        matchedCallStatus: firstNonEmpty(duplicateDialMatchedCall && duplicateDialMatchedCall.status),
                        matchedCallAgeSeconds: duplicateDialMatchedCall ? duplicateDialMatchedCall.ageSeconds : null,
                        activeProbeReason: firstNonEmpty(duplicateDialDiagnostics && duplicateDialDiagnostics.activeReason),
                        recentProbeReason: firstNonEmpty(duplicateDialDiagnostics && duplicateDialDiagnostics.recentReason)
                    }
                });

                return {
                    sent: false,
                    duplicateBlocked: true,
                    reason: duplicateDialReason,
                    hint: duplicateDialHint,
                    source: duplicateDialSource,
                    matchedCall: duplicateDialMatchedCall
                };
            }

            console.log(`Call error (non-blocking): ${error.message}`);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'call_failed',
                problemId: targetProblemId,
                payload: {
                    specialistPhone: toNumber,
                    callStatus: 'failed',
                    level: nextLevel,
                    attempt: nextAttempt,
                    levelIndex: nextIndex,
                    error: truncateText(error.message || 'unknown error', 260)
                }
            });
            return {
                sent: false,
                duplicateBlocked: false,
                reason: 'call_create_failed'
            };
        }
    };

    const getEscalationTarget = (currentLevel) => {
        if (String(currentLevel) === '1' && level2List[0]) {
            return {
                toNumber: level2List[0],
                level: '2',
                attempt: 1,
                levelIndex: 0
            };
        }

        return null;
    };

    const escalateFromLevel = async ({ currentLevel, currentAttempt, currentLevelIndex, callStatus, reason }) => {
        if (String(currentLevel) === '1' && effectiveDisableSmokeEscalation) {
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'smoke_escalation_skipped',
                problemId,
                payload: {
                    callStatus: firstNonEmpty(callStatus, 'unknown'),
                    fromLevel: String(currentLevel),
                    fromAttempt: currentAttempt,
                    fromLevelIndex: currentLevelIndex,
                    reason: firstNonEmpty(reason, 'smoke_escalation_disabled')
                }
            });

            return { escalated: false, reason: 'smoke_escalation_disabled' };
        }

        const escalationTarget = getEscalationTarget(currentLevel);

        if (!escalationTarget) {
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'escalation_skipped',
                problemId,
                payload: {
                    callStatus: firstNonEmpty(callStatus, 'unknown'),
                    fromLevel: String(currentLevel),
                    fromAttempt: currentAttempt,
                    fromLevelIndex: currentLevelIndex,
                    reason: firstNonEmpty(reason, 'no_next_level_configured'),
                    hasLevel2: Boolean(level2List[0])
                }
            });

            return { escalated: false, reason: 'no_next_level_configured' };
        }

        const shouldAttemptPersistentReserve = strictIdempotency || Boolean(appWebhookUrl);

        if (shouldAttemptPersistentReserve) {
            try {
                const reserveResult = await reserveEscalationToApp({
                    appWebhookUrl,
                    appWebhookSecret,
                    problemId,
                    calledNumber: escalationTarget.toNumber,
                    level: escalationTarget.level,
                    reason: firstNonEmpty(reason, callStatus, 'escalation')
                });

                if (!reserveResult.delivered) {
                    const reserveNotDeliveredReason = firstNonEmpty(reserveResult.reason, 'escalation_reserve_not_delivered');
                    if (strictIdempotency) {
                        return { escalated: false, reason: reserveNotDeliveredReason };
                    }

                    console.log(`Escalation reserve soft-failed: ${reserveNotDeliveredReason}`);
                    await postMonitoringEvent({
                        monitoringBackendUrls,
                        ingestToken: monitoringIngestToken,
                        eventType: 'idempotency_guard_soft_failed',
                        problemId,
                        payload: {
                            callStatus: 'degraded',
                            reason: reserveNotDeliveredReason
                        }
                    });
                }

                if (reserveResult.delivered && !reserveResult.reserved) {
                    await postMonitoringEvent({
                        monitoringBackendUrls,
                        ingestToken: monitoringIngestToken,
                        eventType: 'escalation_duplicate_ignored',
                        problemId,
                        payload: {
                            callStatus: firstNonEmpty(callStatus, 'unknown'),
                            fromLevel: String(currentLevel),
                            fromAttempt: currentAttempt,
                            fromLevelIndex: currentLevelIndex,
                            toLevel: escalationTarget.level,
                            toLevelIndex: escalationTarget.levelIndex,
                            reason: firstNonEmpty(reserveResult.reason, 'escalation_already_reserved')
                        }
                    });

                    return { escalated: false, reason: firstNonEmpty(reserveResult.reason, 'escalation_already_reserved') };
                }
            } catch (reserveError) {
                if (!strictIdempotency) {
                    console.log(`Escalation reserve soft-error (non-strict): ${reserveError.message}`);
                }

                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: strictIdempotency ? 'idempotency_guard_blocked' : 'idempotency_guard_soft_failed',
                    problemId,
                    payload: {
                        callStatus: strictIdempotency ? 'blocked' : 'degraded',
                        reason: strictIdempotency ? 'escalation_reserve_failed' : 'escalation_reserve_failed_non_strict',
                        error: truncateText(reserveError.message || 'unknown error', 260)
                    }
                });

                if (strictIdempotency) {
                    return { escalated: false, reason: 'escalation_reserve_failed' };
                }
            }
        }

        const escalationLockKey = `escalation:${problemId}:${escalationTarget.level}:${normalizePhoneNumber(escalationTarget.toNumber)}`;
        if (!acquireFlowLock(escalationLockKey, flowLockTtlMs)) {
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'escalation_duplicate_ignored',
                problemId,
                payload: {
                    callStatus: firstNonEmpty(callStatus, 'unknown'),
                    fromLevel: String(currentLevel),
                    fromAttempt: currentAttempt,
                    fromLevelIndex: currentLevelIndex,
                    toLevel: escalationTarget.level,
                    toLevelIndex: escalationTarget.levelIndex,
                    reason: firstNonEmpty(reason, 'duplicate_escalation')
                }
            });

            return { escalated: false, reason: 'duplicate_escalation_ignored' };
        }

        console.log(`Escalating incident ${problemId} from level ${currentLevel} to level ${escalationTarget.level} due to ${reason || callStatus || 'unknown'}`);

        const escalationDialResult = await tryDial(
            escalationTarget.toNumber,
            escalationTarget.level,
            escalationTarget.attempt,
            escalationTarget.levelIndex
        );
        const callSent = Boolean(escalationDialResult && escalationDialResult.sent);
        const smsSent = callSent
            ? await trySendSms(
                escalationTarget.toNumber,
                escalationTarget.level,
                escalationTarget.attempt,
                firstNonEmpty(callStatus, reason, 'escalated')
            )
            : false;
        const specialistName = resolveSpecialistName(context, payload, escalationTarget.toNumber);

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: callSent ? 'call_escalated' : 'call_escalation_failed',
            problemId,
            payload: {
                specialistPhone: escalationTarget.toNumber,
                specialistName,
                callStatus: callSent ? 'initiated' : 'failed',
                smsSent,
                fromLevel: String(currentLevel),
                fromAttempt: currentAttempt,
                fromLevelIndex: currentLevelIndex,
                toLevel: escalationTarget.level,
                toAttempt: escalationTarget.attempt,
                toLevelIndex: escalationTarget.levelIndex,
                reason: firstNonEmpty(reason, callStatus, 'escalation')
            }
        });

        return {
            escalated: callSent,
            smsSent,
            ...escalationTarget
        };
    };

    const sendTeamsNotification = async () => {
        if (!teamsWebhookUrl) {
            console.log('Missing TEAMS_WEBHOOK_URL; Teams notification not sent');
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'teams_skipped',
                problemId,
                payload: {
                    teamsSent: false,
                    callStatus: 'skipped',
                    reason: 'missing_teams_webhook_url'
                }
            });
            return false;
        }

        const response = await fetch(teamsWebhookUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(buildTeamsPayload(incidentSummary, payload, problemId))
        });

        if (!response.ok) {
            const body = await response.text();
            throw new Error(`Teams notification failed with status ${response.status}: ${body}`);
        }

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'teams_sent',
            problemId,
            payload: {
                teamsSent: true,
                callStatus: 'initiated'
            }
        });

        return true;
    };

    const trySendTeamsNotification = async () => {
        try {
            return await sendTeamsNotification();
        } catch (error) {
            console.log(`Teams notification error (non-blocking): ${error.message}`);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'teams_failed',
                problemId,
                payload: {
                    teamsSent: false,
                    callStatus: 'failed',
                    error: truncateText(error.message || 'unknown error', 260)
                }
            });
            return false;
        }
    };

    try {
        const hasCallSid = Boolean(firstNonEmpty(event.CallSid, payload.CallSid, payload.callSid));

        if (mode === 'start' && hasCallSid) {
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'callback_mode_missing_ignored',
                problemId,
                payload: {
                    callStatus: firstNonEmpty(event.CallStatus, payload.CallStatus, payload.callStatus, 'unknown'),
                    callSid: firstNonEmpty(event.CallSid, payload.CallSid, payload.callSid),
                    reason: hasExplicitMode
                        ? 'start_mode_with_callsid_ignored'
                        : 'callback_like_payload_without_mode'
                }
            });

            return callback(null, 'Ignored start request carrying CallSid');
        }

        if (mode === 'retry') {
            const cronSecret = firstNonEmpty(event.cronSecret, payload.cronSecret);
            const expectedSecret = context.RETRY_CRON_SECRET;
            if (expectedSecret && cronSecret !== expectedSecret) {
                console.log('[RETRY] Unauthorized retry attempt — bad cronSecret');
                return callback(null, 'Unauthorized');
            }

            const syncServiceSid = context.SYNC_SERVICE_SID;
            const retryDelayMinutes = asPositiveInt(firstNonEmpty(context.RETRY_DELAY_MINUTES, '30'), 30);

            const dueRetries = await getDuePendingRetries({ client, syncServiceSid });
            console.log(`[RETRY] Found ${dueRetries.length} due retries`);

            for (const item of dueRetries) {
                const { problemId: rProblemId, level: rLevel, attempt: rAttempt, toNumber: rTo } = item.data;

                // Marcar como procesando para evitar doble ejecución
                try {
                    await client.sync.v1.services(syncServiceSid)
                        .syncMaps('pending_retries')
                        .syncMapItems(item.key)
                        .update({ data: { ...item.data, status: 'processing' } });
                } catch (lockErr) {
                    console.log(`[RETRY] Could not lock item ${item.key}: ${lockErr.message}`);
                    continue;
                }

                const maxLevel1Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL1_ATTEMPTS, '3'), 3);
                const maxLevel2Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL2_ATTEMPTS, '3'), 3);
                const maxAttemptsForLevel = String(rLevel) === '1' ? maxLevel1Attempts : maxLevel2Attempts;
                console.log(`[RETRY] Processing: ${item.key} (L${rLevel} attempt ${rAttempt}/${maxAttemptsForLevel})`);

                try {
                    const dialResult = await tryDial(rTo, rLevel, rAttempt, 0, rProblemId, true);
                    if (dialResult && dialResult.sent) {
                        console.log(`[RETRY] Call initiated for ${item.key}`);
                    } else {
                        console.log(`[RETRY] Dial skipped or failed for ${item.key}`);
                    }
                } catch (callErr) {
                    console.log(`[RETRY] Call failed for ${item.key}: ${callErr.message}`);
                }

                // Eliminar item procesado del Sync
                try {
                    await client.sync.v1.services(syncServiceSid)
                        .syncMaps('pending_retries')
                        .syncMapItems(item.key)
                        .remove();
                } catch (rmErr) {
                    console.log(`[RETRY] Could not remove item ${item.key}: ${rmErr.message}`);
                }
            }

            return callback(null, `Retry checker processed ${dueRetries.length} items`);
        }

        if (mode === 'amd') {
            const callSid = firstNonEmpty(event.CallSid, payload.CallSid);
            const toNumber = firstNonEmpty(event.To, payload.to, payload.To);
            const answeredBy = firstNonEmpty(event.AnsweredBy, payload.AnsweredBy, payload.answeredBy);
            const auditRecord = buildAmdAuditRecord({
                problemId,
                callSid,
                level,
                attempt,
                levelIndex,
                toNumber,
                callStatus: event.CallStatus || payload.CallStatus || 'in-progress',
                answeredBy,
                source: 'async-amd-callback'
            });

            console.log('Twilio AMD audit:', JSON.stringify(auditRecord));
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'amd_audit',
                problemId,
                payload: {
                    specialistPhone: auditRecord.toNumber,
                    callSid: auditRecord.callSid,
                    callStatus: auditRecord.callStatus,
                    answered: auditRecord.amdClassification === 'human',
                    answeredBy: auditRecord.answeredBy,
                    amdClassification: auditRecord.amdClassification,
                    level: auditRecord.level,
                    attempt: auditRecord.attempt,
                    levelIndex: auditRecord.levelIndex
                }
            });
            return callback(null, `AMD registrado: ${auditRecord.amdClassification}`);
        }

        if (mode === 'callback') {
            const callStatus = (event.CallStatus || payload.CallStatus || '').toLowerCase();
            const callSid = firstNonEmpty(event.CallSid, payload.CallSid);
            const callbackLockKey = `callback:${problemId}:${level}:${callSid || `${attempt}:${levelIndex}:${callStatus || 'unknown'}`}`;
            const ackConfirmedKey = `ack_confirmed:${problemId}:${callSid || `${level}:${attempt}:${levelIndex}`}`;
            const ackConfirmedFromStatusCallback = asBooleanFlag(firstNonEmpty(
                event.ackConfirmed,
                payload.ackConfirmed,
                payload.ack_confirmed,
                event.AckConfirmed,
                payload.AckConfirmed
            ), false);
            const ackConfirmedFromMemory = hasActiveFlowLock(ackConfirmedKey);

            logCallStatusTrace(traceCallStatus, 'callback_inbound', {
                problemId,
                mode,
                level,
                attempt,
                levelIndex,
                callSid,
                eventCallStatus: firstNonEmpty(event.CallStatus),
                payloadCallStatus: firstNonEmpty(payload.CallStatus, payload.callStatus),
                rawAnsweredBy: firstNonEmpty(event.AnsweredBy, payload.AnsweredBy, payload.answeredBy)
            });

            if (!acquireFlowLock(callbackLockKey, flowLockTtlMs)) {
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'callback_duplicate_ignored',
                    problemId,
                    payload: {
                        callSid,
                        callStatus: firstNonEmpty(callStatus, 'unknown'),
                        level,
                        attempt,
                        levelIndex
                    }
                });
                return callback(null, `Duplicate callback ignored for call ${callSid || 'unknown'}`);
            }

            const currentList = level === '2' ? level2List : level1List;
            const toNumber = currentList[levelIndex];
            const answeredBy = firstNonEmpty(
                event.AnsweredBy,
                payload.AnsweredBy,
                await getAnsweredByFromCall(client, callSid)
            );
            const auditRecord = buildAmdAuditRecord({
                problemId,
                callSid,
                level,
                attempt,
                levelIndex,
                toNumber,
                callStatus,
                answeredBy,
                source: 'status-callback'
            });

            console.log(`Callback level ${level} attempt ${attempt} index ${levelIndex} status ${callStatus}`);
            console.log('Twilio call audit:', JSON.stringify(auditRecord));

            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'call_audit',
                problemId,
                payload: {
                    specialistPhone: auditRecord.toNumber,
                    callSid: auditRecord.callSid,
                    callStatus: auditRecord.callStatus,
                    answered: requireDtmfAck ? false : auditRecord.amdClassification === 'human',
                    answeredBy: auditRecord.answeredBy,
                    amdClassification: auditRecord.amdClassification,
                    level: auditRecord.level,
                    attempt: auditRecord.attempt,
                    levelIndex: auditRecord.levelIndex
                }
            });

            const amdClassification = classifyAmdResult(answeredBy);
            const ackConfirmed = ackConfirmedFromStatusCallback || ackConfirmedFromMemory;
            const callbackEscalationCandidate = requireDtmfAck
                ? shouldRetryCall(callStatus)
                : shouldContinueEscalation(callStatus, answeredBy);
            const shouldEscalateFromCallback = callbackEscalationCandidate && !effectiveDisableSmokeEscalation;

            logCallStatusTrace(traceCallStatus, 'callback_decision', {
                problemId,
                level,
                attempt,
                levelIndex,
                callSid,
                callStatus,
                answeredBy,
                amdClassification,
                ackConfirmedFromStatusCallback,
                ackConfirmedFromMemory,
                ackConfirmed,
                requireDtmfAck,
                callbackEscalationCandidate,
                effectiveDisableSmokeEscalation,
                shouldEscalateFromCallback
            });

            if (shouldEscalateFromCallback) {
                const syncServiceSid = context.SYNC_SERVICE_SID;
                const maxLevel1Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL1_ATTEMPTS, '3'), 3);
                const maxLevel2Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL2_ATTEMPTS, '3'), 3);
                const retryDelayMinutes = asPositiveInt(firstNonEmpty(context.RETRY_DELAY_MINUTES, '30'), 30);
                const maxAttemptsForCurrentLevel = String(level) === '1' ? maxLevel1Attempts : maxLevel2Attempts;

                if (attempt < maxAttemptsForCurrentLevel) {
                    // Hay más intentos disponibles → agendar reintento
                    const scheduled = await scheduleRetry({
                        client, syncServiceSid, problemId,
                        level, attempt, toNumber,
                        delayMinutes: retryDelayMinutes
                    });
                    console.log(`[RETRY] ${scheduled ? 'Scheduled' : 'Already scheduled'}: L${level} retry ${attempt + 1} in ${retryDelayMinutes} min`);
                    await postMonitoringEvent({
                        monitoringBackendUrls,
                        ingestToken: monitoringIngestToken,
                        eventType: 'retry_scheduled',
                        problemId,
                        payload: {
                            specialistPhone: toNumber,
                            callStatus: firstNonEmpty(callStatus, 'unknown'),
                            level,
                            attempt,
                            nextAttempt: attempt + 1,
                            retryDelayMinutes,
                            scheduled
                        }
                    });
                    return callback(null, `Retry scheduled: L${level} attempt ${attempt + 1} in ${retryDelayMinutes} min`);
                }

                // Agotados intentos de nivel 1 → escalar a nivel 2
                if (String(level) === '1' && level2List[0]) {
                    console.log(`[RETRY] Level 1 exhausted (${maxLevel1Attempts} attempts) → escalating to Level 2 immediately`);
                    const escalationResult = await escalateFromLevel({
                        currentLevel: level,
                        currentAttempt: attempt,
                        currentLevelIndex: levelIndex,
                        callStatus: firstNonEmpty(callStatus, 'unknown'),
                        reason: 'max_level1_attempts_reached'
                    });
                    if (escalationResult && escalationResult.escalated) {
                        return callback(null, `Escalated to Level 2 after ${maxLevel1Attempts} Level 1 attempts`);
                    }
                    return callback(null, `Level 2 escalation skipped: ${(escalationResult && escalationResult.reason) || 'unknown'}`);
                }

                // Agotados intentos de nivel 2 → fin del flujo
                console.log(`[RETRY] Level ${level} exhausted (${maxAttemptsForCurrentLevel} attempts) → incident closed, no more escalation`);
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'escalation_exhausted',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callStatus: firstNonEmpty(callStatus, 'unknown'),
                        level,
                        attempt,
                        reason: 'max_attempts_reached_all_levels'
                    }
                });
                return callback(null, `Max attempts reached for Level ${level} — incident escalation complete`);
            }

            if (!requireDtmfAck && shouldTreatCallAsHuman(callStatus, answeredBy)) {
                return callback(null, `Call answered by human on level ${level} attempt ${attempt}`);
            }

            if (requireDtmfAck) {
                return callback(null, `Call completed on level ${level} attempt ${attempt}; DTMF acknowledgment required`);
            }

            return callback(null, `Single-attempt mode: no retry/escalation for status ${callStatus || 'unknown'} with AMD ${classifyAmdResult(answeredBy)}`);
        }

        if (mode === 'ack') {
            const digit = firstNonEmpty(event.Digits, payload.Digits);
            const callSid = firstNonEmpty(event.CallSid, payload.CallSid);
            const currentList = level === '2' ? level2List : level1List;
            const toNumber = firstNonEmpty(event.To, payload.to, payload.To, payload.toNumber, currentList[levelIndex]);
            const replayCount = asPositiveInt(firstNonEmpty(payload.replayCount, event.replayCount, '0'), 0);
            const sayMessage = level === '2' ? level2Message : message;
            const ackLockKey = `ack:${problemId}:${callSid || `${level}:${attempt}:${levelIndex}`}:${digit || 'none'}:${replayCount}`;
            const ackConfirmedKey = `ack_confirmed:${problemId}:${callSid || `${level}:${attempt}:${levelIndex}`}`;

            logCallStatusTrace(traceCallStatus, 'ack_inbound', {
                problemId,
                mode,
                level,
                attempt,
                levelIndex,
                callSid,
                digit: digit || '',
                replayCount,
                eventCallStatus: firstNonEmpty(event.CallStatus),
                payloadCallStatus: firstNonEmpty(payload.CallStatus, payload.callStatus)
            });

            if (!acquireFlowLock(ackLockKey, flowLockTtlMs)) {
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'ack_duplicate_ignored',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callSid,
                        callStatus: 'duplicate_ignored',
                        digit: digit || '',
                        replayCount,
                        level,
                        attempt,
                        levelIndex
                    }
                });

                const duplicateAckTwiml = buildSimpleTwiml({
                    message: 'Esta confirmación ya fue procesada. Finalizamos la llamada.',
                    voice: callMessageVoice,
                    language: callMessageLanguage
                });

                return callback(null, buildXmlCallbackResponse(duplicateAckTwiml));
            }

            if (digit === '1') {
                setFlowLock(ackConfirmedKey, flowLockTtlMs);

                const ackPersistedInCall = await persistAckConfirmationOnCall({
                    client,
                    callSid,
                    functionBaseUrl,
                    problemId,
                    level,
                    attempt,
                    levelIndex
                });

                logCallStatusTrace(traceCallStatus, 'ack_confirmed', {
                    problemId,
                    level,
                    attempt,
                    levelIndex,
                    callSid,
                    ackPersistedInCall
                });

                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'call_acknowledged',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callSid,
                        callStatus: 'acknowledged',
                        answered: true,
                        digit,
                        ackPersistedInCall,
                        level,
                        attempt,
                        levelIndex
                    }
                });

                try {
                    await postIncidentAckToApp({
                        appWebhookUrl,
                        appWebhookSecret,
                        problemId,
                        calledNumber: toNumber,
                        incidentAttended: true,
                        incidentStatus: 'ACKNOWLEDGED'
                    });
                } catch (appError) {
                    console.log(`Monitoring app ack update error (non-blocking): ${appError.message}`);
                }

                const confirmedTwiml = buildSimpleTwiml({
                    message: 'Recepción confirmada. Gracias. Finalizamos la llamada.',
                    voice: callMessageVoice,
                    language: callMessageLanguage
                });

                return callback(null, buildXmlCallbackResponse(confirmedTwiml));
            }

            if (digit === '2' && replayCount < ackMaxReplays) {
                const nextReplayCount = replayCount + 1;
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'call_message_replayed',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callSid,
                        callStatus: 'replayed',
                        answered: false,
                        digit,
                        replayCount: nextReplayCount,
                        level,
                        attempt,
                        levelIndex
                    }
                });

                const replayActionUrl = buildAckActionUrl({
                    functionBaseUrl,
                    problemId,
                    level,
                    attempt,
                    levelIndex,
                    toNumber,
                    replayCount: nextReplayCount
                });

                const replayTwiml = buildInteractiveCallTwiml({
                    sayMessage,
                    actionUrl: replayActionUrl,
                    voice: callMessageVoice,
                    language: callMessageLanguage
                });

                return callback(null, buildXmlCallbackResponse(replayTwiml));
            }

            const unansweredReason = digit
                ? (digit === '2' ? 'max_replays_reached' : `invalid_digit_${digit}`)
                : 'no_digit';

            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'call_not_acknowledged',
                problemId,
                payload: {
                    specialistPhone: toNumber,
                    callSid,
                    callStatus: 'not_acknowledged',
                    answered: false,
                    digit: digit || '',
                    reason: unansweredReason,
                    replayCount,
                    level,
                    attempt,
                    levelIndex
                }
            });

            try {
                await postIncidentAckToApp({
                    appWebhookUrl,
                    appWebhookSecret,
                    problemId,
                    calledNumber: toNumber,
                    incidentAttended: false,
                    incidentStatus: 'NOT_ACKNOWLEDGED'
                });
            } catch (appError) {
                console.log(`Monitoring app ack update error (non-blocking): ${appError.message}`);
            }

            const resolvedAckCallStatus = String(firstNonEmpty(
                event.CallStatus,
                payload.CallStatus,
                payload.callStatus,
                await getCallStatusFromCall(client, callSid)
            ) || '').toLowerCase();
            const resolvedAckAnsweredBy = firstNonEmpty(
                event.AnsweredBy,
                payload.AnsweredBy,
                payload.answeredBy,
                await getAnsweredByFromCall(client, callSid)
            );
            const resolvedAckAmdClassification = classifyAmdResult(resolvedAckAnsweredBy);
            const callbackOwnsEscalationCandidate = shouldRetryCall(resolvedAckCallStatus)
                || (Boolean(callSid) && !resolvedAckCallStatus);
            const callbackOwnsEscalation = callbackOwnsEscalationCandidate && !effectiveDisableSmokeEscalation;

            logCallStatusTrace(traceCallStatus, 'ack_decision', {
                problemId,
                level,
                attempt,
                levelIndex,
                callSid,
                digit: digit || '',
                replayCount,
                resolvedAckCallStatus,
                resolvedAckAnsweredBy,
                resolvedAckAmdClassification,
                ackConfirmed: hasActiveFlowLock(ackConfirmedKey),
                callbackOwnsEscalationCandidate,
                effectiveDisableSmokeEscalation,
                callbackOwnsEscalation
            });

            if (callbackOwnsEscalation) {
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'ack_escalation_deferred_to_callback',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callSid,
                        callStatus: firstNonEmpty(resolvedAckCallStatus, 'unknown'),
                        level,
                        attempt,
                        levelIndex
                    }
                });
            }

            let escalationResult = null;
            if (String(level) === '1' && !callbackOwnsEscalation && !effectiveDisableSmokeEscalation) {
                const syncServiceSid = context.SYNC_SERVICE_SID;
                const maxLevel1Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL1_ATTEMPTS, '3'), 3);
                const maxLevel2Attempts = asPositiveInt(firstNonEmpty(context.MAX_LEVEL2_ATTEMPTS, '3'), 3);
                const retryDelayMinutes = asPositiveInt(firstNonEmpty(context.RETRY_DELAY_MINUTES, '30'), 30);
                const maxAttemptsForCurrentLevel = String(level) === '1' ? maxLevel1Attempts : maxLevel2Attempts;

                if (attempt < maxAttemptsForCurrentLevel) {
                    const scheduled = await scheduleRetry({
                        client, syncServiceSid, problemId,
                        level, attempt, toNumber,
                        delayMinutes: retryDelayMinutes
                    });
                    console.log(`[RETRY_ACK] ${scheduled ? 'Scheduled' : 'Already scheduled'}: L${level} retry ${attempt + 1} in ${retryDelayMinutes} min`);
                    escalationResult = { escalated: false, reason: 'retry_scheduled' };
                } else if (String(level) === '1' && level2List[0]) {
                    console.log(`[RETRY_ACK] Level 1 exhausted → escalating to Level 2`);
                    escalationResult = await escalateFromLevel({
                        currentLevel: level,
                        currentAttempt: attempt,
                        currentLevelIndex: levelIndex,
                        callStatus: 'not_acknowledged',
                        reason: 'max_level1_attempts_reached_ack'
                    });
                } else {
                    console.log(`[RETRY_ACK] Level ${level} exhausted → incident closed`);
                    escalationResult = { escalated: false, reason: 'max_attempts_all_levels' };
                }
            }

            let unansweredMessage = 'No recibimos una confirmación válida. La llamada será considerada como no atendida.';
            if (String(level) === '1') {
                if (effectiveDisableSmokeEscalation) {
                    unansweredMessage = 'No recibimos una confirmación válida. Para pruebas controladas, no se escalará automáticamente a nivel 2.';
                } else if (callbackOwnsEscalation) {
                    unansweredMessage = 'La llamada terminó sin confirmación válida. Escalaremos automáticamente al soporte de nivel 2 por estado de corte o detección automática.';
                } else {
                    unansweredMessage = escalationResult && escalationResult.escalated
                        ? 'No recibimos una confirmación válida. Escalaremos automáticamente al soporte de nivel 2.'
                        : 'No recibimos una confirmación válida. No fue posible escalar automáticamente al soporte de nivel 2.';
                }
            }

            const unansweredTwiml = buildSimpleTwiml({
                message: unansweredMessage,
                voice: callMessageVoice,
                language: callMessageLanguage
            });

            return callback(null, buildXmlCallbackResponse(unansweredTwiml));
        }

        if (!isCriticalAlert(payload, incidentSummary)) {
            const severity = getNormalizedSeverity(payload) || 'unknown';
            const hasRequiredTag = hasObservabilityCriticalTag(payload);
            const effectiveCriticality = getEffectiveCriticality(payload) || 'empty';
            const skipReason = `Ignored alert: severity=${severity}, hasRequiredTag=${hasRequiredTag}, effectiveCriticality=${effectiveCriticality}`;
            console.log(skipReason);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'alert_ignored',
                problemId,
                payload: {
                    callStatus: 'ignored',
                    severity,
                    hasRequiredTag,
                    effectiveCriticality,
                    title: incidentSummary.title,
                    reason: skipReason
                }
            });
            return callback(null, skipReason);
        }

        if (!allowedStatusForNotification) {
            const normalizedStatus = firstNonEmpty(payload.status, payload.State, payload.state, payload.problemStatus, payload['event.status']) || 'unknown';
            const skipReason = `Ignored alert by status filter: status=${normalizedStatus}`;
            console.log(skipReason);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'alert_ignored_by_status',
                problemId,
                payload: {
                    callStatus: 'ignored',
                    status: normalizedStatus,
                    title: incidentSummary.title,
                    reason: skipReason
                }
            });
            return callback(null, skipReason);
        }

        if (strictIdempotency && !appWebhookUrl) {
            const blockedReason = 'Strict idempotency requires APP_WEBHOOK_URL to dedupe calls across serverless invocations';
            console.log(blockedReason);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'idempotency_guard_blocked',
                problemId,
                payload: {
                    callStatus: 'blocked',
                    reason: 'missing_app_webhook_url'
                }
            });
            return callback(null, blockedReason);
        }

        if (strictIdempotency && problemId === 'unknown') {
            const blockedReason = 'Strict idempotency blocked call because a stable problem identifier could not be resolved';
            console.log(blockedReason);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'idempotency_guard_blocked',
                problemId,
                payload: {
                    callStatus: 'blocked',
                    reason: 'missing_stable_problem_id'
                }
            });
            return callback(null, blockedReason);
        }

        if (!acquireFlowLock(startLockKey, flowLockTtlMs)) {
            const duplicateStartReason = `Duplicate start ignored for incident ${problemId}`;
            console.log(duplicateStartReason);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'start_duplicate_ignored',
                problemId,
                payload: {
                    callStatus: 'ignored',
                    reason: duplicateStartReason,
                    severity: getNormalizedSeverity(payload),
                    title: incidentSummary.title
                }
            });
            return callback(null, duplicateStartReason);
        }

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'incident_received',
            problemId,
            payload: {
                specialistPhone: level1List[0] || level2List[0] || '',
                specialistName: '',
                callStatus: 'received',
                answered: false,
                smsSent: false,
                teamsSent: false,
                severity: getNormalizedSeverity(payload),
                priority: incidentSummary.priority,
                title: incidentSummary.title,
                affectedEntity: incidentSummary.affectedEntity,
                incidentArea: incidentSummary.incidentArea || '',
                sourceStatus: firstNonEmpty(payload.status, payload.State, payload.state, payload.problemStatus, payload['event.status']) || 'OPEN'
            }
        });

        
        // SIMPLIFIED DEDUPLICATION: Check if we already sent an SMS for this problemId
        if (smsFromNumber && problemId && problemId !== 'unknown') {
            try {
                const initialLevelForCheck = String(payload.initialLevel || payload.routingLevel || '').trim() === '2' ? '2' : '1';
                const listForCheck = initialLevelForCheck === '2' ? level2List : level1List;
                const numberForCheck = listForCheck[0];
                
                if (numberForCheck) {
                    const recentSms = await client.messages.list({ to: numberForCheck, limit: 10 });
                    const alreadyProcessed = recentSms.some(msg => msg.body && msg.body.includes(`ID: ${problemId}`));
                    if (alreadyProcessed) {
                        const skipReason = `Incident ${problemId} already processed (found in SMS history). Ignoring Dynatrace retry.`;
                        console.log(skipReason);
                        return callback(null, skipReason);
                    }
                }
            } catch (e) {
                console.log(`Failed to check SMS history for deduplication: ${e.message}`);
            }
        }
        
        const requestedInitialLevel = String(payload.initialLevel || payload.routingLevel || '').trim();

        const initialLevel = requestedInitialLevel === '2' ? '2' : '1';
        const initialList = initialLevel === '2' ? level2List : level1List;
        let toNumber = initialList[0];

        if (!toNumber && initialLevel === '2' && level1List[0]) {
            console.log('Level 2 not configured on start; falling back to level 1');
            toNumber = level1List[0];
        }

        if (!toNumber) {
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'routing_failed',
                problemId,
                payload: {
                    callStatus: 'failed',
                    reason: 'no_level_1_number_configured',
                    hasLevel1: Boolean(level1List[0]),
                    hasLevel2: Boolean(level2List[0])
                }
            });
            return callback(new Error('No level 1 number configured'));
        }

        const effectiveInitialLevel = initialLevel === '2' && !initialList[0] ? '1' : initialLevel;
        const calledPersonName = resolveSpecialistName(context, payload, toNumber);

        const shouldRunSmokeSingleCallGuard = isSmokeLikeProblemId && enforceSmokeSingleCall && !dialGuardBypassActive;
        if (shouldRunSmokeSingleCallGuard) {
            const smokeSingleCallProbe = await hasExistingOutboundDial({
                client,
                toNumber,
                fromNumber,
                windowSeconds: smokeSingleCallWindowSeconds,
                maxResults: recentDialGuardMaxResults,
                includeRecentLookup: true
            });

            logCallStatusTrace(traceCallStatus, 'smoke_single_call_probe', {
                problemId,
                toNumber,
                fromNumber,
                level: effectiveInitialLevel,
                attempt: 1,
                levelIndex: 0,
                matched: Boolean(smokeSingleCallProbe && smokeSingleCallProbe.matched),
                reason: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.reason),
                source: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.source),
                hint: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.hint),
                matchedCallSid: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.matchedCall && smokeSingleCallProbe.matchedCall.sid),
                matchedCallStatus: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.matchedCall && smokeSingleCallProbe.matchedCall.status),
                matchedCallAgeSeconds: smokeSingleCallProbe && smokeSingleCallProbe.matchedCall ? smokeSingleCallProbe.matchedCall.ageSeconds : null,
                guardWindowSeconds: smokeSingleCallWindowSeconds,
                activeProbeReason: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.diagnostics && smokeSingleCallProbe.diagnostics.activeReason),
                recentProbeReason: firstNonEmpty(smokeSingleCallProbe && smokeSingleCallProbe.diagnostics && smokeSingleCallProbe.diagnostics.recentReason)
            });

            if (smokeSingleCallProbe.matched) {
                const blockedHint = firstNonEmpty(smokeSingleCallProbe.hint);
                const blockedReason = `SMOKE single-call guard blocked dial for ${toNumber} within ${smokeSingleCallWindowSeconds}s window${blockedHint ? `. Hint: ${blockedHint}` : ''}`;
                console.log(blockedReason);

                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'smoke_single_call_blocked',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callStatus: 'blocked',
                        level: effectiveInitialLevel,
                        attempt: 1,
                        levelIndex: 0,
                        guardWindowSeconds: smokeSingleCallWindowSeconds,
                        reason: firstNonEmpty(smokeSingleCallProbe.reason, 'smoke_single_call_guard_match'),
                        hint: blockedHint,
                        guardSource: firstNonEmpty(smokeSingleCallProbe.source, 'unknown'),
                        matchedCallSid: firstNonEmpty(smokeSingleCallProbe.matchedCall && smokeSingleCallProbe.matchedCall.sid),
                        matchedCallStatus: firstNonEmpty(smokeSingleCallProbe.matchedCall && smokeSingleCallProbe.matchedCall.status),
                        matchedCallAgeSeconds: smokeSingleCallProbe.matchedCall ? smokeSingleCallProbe.matchedCall.ageSeconds : null,
                        activeProbeReason: firstNonEmpty(smokeSingleCallProbe.diagnostics && smokeSingleCallProbe.diagnostics.activeReason),
                        recentProbeReason: firstNonEmpty(smokeSingleCallProbe.diagnostics && smokeSingleCallProbe.diagnostics.recentReason)
                    }
                });

                return callback(null, blockedReason);
            }
        }

        const includeRecentLookupForStart = enforceRecentWindowGuard || smokeStartRecentGuardActive;
        const startGuardWindowSeconds = includeRecentLookupForStart
            ? (
                effectiveEnforceSingleDialPerLevel
                    ? effectiveSingleDialWindowSeconds
                    : (smokeStartRecentGuardActive && !enforceRecentWindowGuard
                        ? smokeStartRecentGuardWindowSeconds
                        : recentDialGuardWindowSeconds)
            )
            : recentDialGuardWindowSeconds;
        const shouldRunStartDialGuard = (enableActiveDialGuard || includeRecentLookupForStart)
            && !dialGuardBypassActive
            && !shouldRunSmokeSingleCallGuard;
        if (dialGuardBypassActive) {
            logCallStatusTrace(traceCallStatus, 'start_guard_bypass', {
                problemId,
                toNumber,
                fromNumber,
                level: effectiveInitialLevel,
                attempt: 1,
                levelIndex: 0,
                reason: 'smoke_test_bypass_active'
            });
        }
        if (shouldRunStartDialGuard) {
            const duplicateStartDial = await hasExistingOutboundDial({
                client,
                toNumber,
                fromNumber,
                windowSeconds: startGuardWindowSeconds,
                maxResults: recentDialGuardMaxResults,
                includeRecentLookup: includeRecentLookupForStart
            });

            logCallStatusTrace(traceCallStatus, 'start_guard_probe', {
                problemId,
                toNumber,
                fromNumber,
                level: effectiveInitialLevel,
                attempt: 1,
                levelIndex: 0,
                matched: Boolean(duplicateStartDial && duplicateStartDial.matched),
                reason: firstNonEmpty(duplicateStartDial && duplicateStartDial.reason),
                source: firstNonEmpty(duplicateStartDial && duplicateStartDial.source),
                hint: firstNonEmpty(duplicateStartDial && duplicateStartDial.hint),
                matchedCallSid: firstNonEmpty(duplicateStartDial && duplicateStartDial.matchedCall && duplicateStartDial.matchedCall.sid),
                matchedCallStatus: firstNonEmpty(duplicateStartDial && duplicateStartDial.matchedCall && duplicateStartDial.matchedCall.status),
                matchedCallAgeSeconds: duplicateStartDial && duplicateStartDial.matchedCall ? duplicateStartDial.matchedCall.ageSeconds : null,
                recentLookupEnabled: includeRecentLookupForStart,
                guardWindowSeconds: includeRecentLookupForStart ? startGuardWindowSeconds : 0,
                activeProbeReason: firstNonEmpty(duplicateStartDial && duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.activeReason),
                recentProbeReason: firstNonEmpty(duplicateStartDial && duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.recentReason)
            });

            if (duplicateStartDial.matched) {
                const guardWindowSeconds = startGuardWindowSeconds;
                const blockedHint = firstNonEmpty(duplicateStartDial.hint);
                const duplicateStartReason = firstNonEmpty(duplicateStartDial.reason, 'guard_match');
                const blockedReason = duplicateStartReason === 'recent_call_in_window'
                    ? `Start blocked by dial guard for ${toNumber} within ${guardWindowSeconds}s window (${duplicateStartReason})${blockedHint ? `. Hint: ${blockedHint}` : ''}`
                    : `Start blocked by dial guard for ${toNumber} (${duplicateStartReason})${blockedHint ? `. Hint: ${blockedHint}` : ''}`;
                console.log(blockedReason);
                console.log(`[DIAL_GUARD_DIAG] ${JSON.stringify({
                    problemId,
                    toNumber,
                    fromNumber,
                    level: effectiveInitialLevel,
                    reason: duplicateStartReason,
                    source: firstNonEmpty(duplicateStartDial.source, 'unknown'),
                    hint: blockedHint,
                    matchedCall: duplicateStartDial.matchedCall || null,
                    diagnostics: duplicateStartDial.diagnostics || null
                })}`);
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'start_duplicate_blocked_recent_guard',
                    problemId,
                    payload: {
                        specialistPhone: toNumber,
                        callStatus: 'blocked',
                        level: effectiveInitialLevel,
                        attempt: 1,
                        levelIndex: 0,
                        guardWindowSeconds: duplicateStartReason === 'recent_call_in_window' ? guardWindowSeconds : 0,
                        reason: blockedReason,
                        hint: blockedHint,
                        guardSource: firstNonEmpty(duplicateStartDial.source, 'unknown'),
                        matchedCallSid: firstNonEmpty(duplicateStartDial.matchedCall && duplicateStartDial.matchedCall.sid),
                        matchedCallStatus: firstNonEmpty(duplicateStartDial.matchedCall && duplicateStartDial.matchedCall.status),
                        matchedCallAgeSeconds: duplicateStartDial.matchedCall ? duplicateStartDial.matchedCall.ageSeconds : null,
                        activeProbeReason: firstNonEmpty(duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.activeReason),
                        recentProbeReason: firstNonEmpty(duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.recentReason)
                    }
                });
                return callback(null, blockedReason);
            }

            logCallStatusTrace(traceCallStatus, 'start_guard_pass', {
                problemId,
                toNumber,
                fromNumber,
                level: effectiveInitialLevel,
                reason: firstNonEmpty(duplicateStartDial.reason, 'no_duplicate_detected'),
                hint: firstNonEmpty(duplicateStartDial.hint),
                activeProbeReason: firstNonEmpty(duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.activeReason),
                recentProbeReason: firstNonEmpty(duplicateStartDial.diagnostics && duplicateStartDial.diagnostics.recentReason)
            });
        }

        try {
            const appIngestResult = await postIncidentToApp({
                appWebhookUrl,
                appWebhookSecret,
                incidentSummary,
                payload,
                problemId,
                calledNumber: toNumber,
                calledPersonName,
                callStatus: 'OPEN',
                phoneSent: false,
                smsSent: false,
                teamsSent: false,
                timeoutMs: 7000,
                retries: 1
            });

            if (appIngestResult.delivered) {
                console.log(`Incident ${problemId} created in monitoring app before call flow`);
            }

            if (strictIdempotency && !appIngestResult.delivered) {
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'idempotency_guard_blocked',
                    problemId,
                    payload: {
                        callStatus: 'blocked',
                        reason: firstNonEmpty(appIngestResult.reason, 'app_webhook_not_delivered')
                    }
                });
                return callback(null, `Start blocked by strict idempotency guard for problem ${problemId}`);
            }

            if (appIngestResult.duplicate || appIngestResult.created === false) {
                await postMonitoringEvent({
                    monitoringBackendUrls,
                    ingestToken: monitoringIngestToken,
                    eventType: 'incident_duplicate_ignored',
                    problemId,
                    payload: {
                        callStatus: 'ignored',
                        reason: firstNonEmpty(appIngestResult.reason, 'problem_id_already_exists')
                    }
                });
                return callback(null, `Duplicate incident ignored for problem ${problemId}`);
            }
        } catch (appError) {
            console.log(`Monitoring app webhook create error (non-blocking): ${appError.message}`);

                if (strictIdempotency) {
                    await postMonitoringEvent({
                        monitoringBackendUrls,
                        ingestToken: monitoringIngestToken,
                        eventType: 'idempotency_guard_blocked',
                        problemId,
                        payload: {
                            callStatus: 'blocked',
                            reason: 'app_webhook_unavailable_during_start_guard',
                            error: truncateText(appError.message || 'unknown error', 260)
                        }
                    });
                    return callback(null, `Start blocked by strict idempotency guard for problem ${problemId}`);
                }
        }

        const initialDialResult = await tryDial(toNumber, effectiveInitialLevel, 1, 0);
        const callSent = Boolean(initialDialResult && initialDialResult.sent);

        const smsSent = callSent ? await trySendSms(toNumber, effectiveInitialLevel, 1, 'initiated') : false;
        const teamsSent = callSent ? await trySendTeamsNotification() : false;

        console.log('Channel status summary:', JSON.stringify({
            problemId,
            phoneSent: callSent,
            smsSent,
            teamsSent
        }));
        const finalStartHint = firstNonEmpty(initialDialResult && initialDialResult.hint);
        const finalStartMessage = callSent
            ? 'Call initiated'
            : `Call skipped: ${firstNonEmpty(initialDialResult && initialDialResult.reason, 'dial_not_sent')}${finalStartHint ? `. Hint: ${finalStartHint}` : ''}`;
        callback(null, finalStartMessage);
    } catch (error) {
        console.log(error);
        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'function_error',
            problemId,
            payload: {
                callStatus: 'failed',
                error: truncateText(error.message || 'unknown error', 260)
            }
        });
        callback(error);
    }
};


