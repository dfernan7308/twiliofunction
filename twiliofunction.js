const xmlEscape = (value) => String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');

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

const hasObservabilityCriticalTag = (payload) => {
    const normalizedNeedles = [
        'custom_call_turno_observabilidad',
        'custom:call_turno_observabilidad',
        'call_turno_observabilidad'
    ];
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

    return tagSources.some((source) => {
        const tags = normalizeTagSource(source);
        return tags.some((tag) => {
            const tagText = typeof tag === 'object'
                ? firstNonEmpty(tag.stringRepresentation, tag.key, tag.value, tag.name, tag.tag, `${tag.context || ''}:${tag.key || ''}`)
                : String(tag || '');

            const normalizedTag = tagText
                .trim()
                .toLowerCase()
                .replace(/[\s\-./]+/g, '_')
                .replace(/:+/g, ':');
            const normalizedTagLoose = normalizedTag.replace(/[:_]/g, '');

            return normalizedNeedles.some((needle) => {
                const normalizedNeedle = needle.toLowerCase();
                const normalizedNeedleLoose = normalizedNeedle.replace(/[:_]/g, '');
                return normalizedTag.includes(normalizedNeedle) || normalizedTagLoose.includes(normalizedNeedleLoose);
            });
        });
    });
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
        title,
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

const buildAmdStatusCallbackUrl = (baseUrl, context, payload, query) => {
    const callbackBaseUrl = firstNonEmpty(
        context.AMD_STATUS_CALLBACK_URL,
        payload.amdStatusCallback,
        payload.asyncAmdStatusCallback,
        baseUrl
    );

    if (!callbackBaseUrl) {
        return '';
    }

    const separator = callbackBaseUrl.includes('?') ? '&' : '?';
    return `${callbackBaseUrl}${separator}${query}`;
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

const buildUrlWithQuery = (baseUrl, query) => {
    if (!baseUrl) {
        return '';
    }

    const separator = baseUrl.includes('?') ? '&' : '?';
    return `${baseUrl}${separator}${query}`;
};

const buildAckActionUrl = ({ functionBaseUrl, problemId, level, attempt, levelIndex, toNumber, replayCount }) => {
    return buildUrlWithQuery(
        functionBaseUrl,
        `mode=ack&problemId=${encodeURIComponent(problemId)}&level=${encodeURIComponent(level)}&attempt=${encodeURIComponent(attempt)}&levelIndex=${encodeURIComponent(levelIndex)}&to=${encodeURIComponent(toNumber || '')}&replayCount=${encodeURIComponent(replayCount)}`
    );
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
        cause_name: incidentSummary.causeName,
        affected_entity: incidentSummary.affectedEntity,
        source: 'twilio-function',
        channels: {
            phone: Boolean(phoneSent),
            sms: Boolean(smsSent),
            teams: Boolean(teamsSent)
        }
    };

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

    return { delivered: true };
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

exports.handler = async function (context, event, callback) {
    let payload = buildIncomingPayload(event);
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

    const problemId = payload.problemId || payload.ProblemID || payload.PROBLEM_ID || payload['event.id'] || 'unknown';
    const mode = payload.mode || 'start';
    const level = payload.level || '1';
    const attempt = parseInt(payload.attempt || '1', 10);
    const levelIndex = parseInt(payload.levelIndex || '0', 10);
    const requireDtmfAck = ['1', 'true', 'yes', 'on'].includes(firstNonEmpty(context.REQUIRE_DTMF_ACK, payload.requireDtmfAck, 'true').toLowerCase());
    const ackMaxReplays = asPositiveInt(firstNonEmpty(context.ACK_MAX_REPLAYS, payload.ackMaxReplays, '2'), 2);
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

    if (shouldEnrichFromDynatrace(context, payload) && requiresDynatraceEnrichment(payload) && problemId !== 'unknown') {
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

    if (!monitoringBackendUrls.length) {
        console.log('MONITORING_BACKEND_URL not configured; dashboard ingestion is disabled');
    }

    if (debugEnabled) {
        console.log('Dynatrace raw event:', JSON.stringify(event));
        console.log('Dynatrace merged payload:', JSON.stringify(payload));
        console.log('Dynatrace payload keys:', JSON.stringify(getObjectKeys(payload)));
        console.log('Dynatrace entity tags:', JSON.stringify(payload.entityTags || payload.EntityTags || payload.tags || payload.entity_tags || payload['Entity tags'] || []));
        console.log('Dynatrace observability tag detected:', hasObservabilityCriticalTag(payload));
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
        affectedEntity: incidentSummary.affectedEntity
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
        const sayMessage = nextLevel === '2' ? level2Message : message;
        const amdStatusCallback = buildAmdStatusCallbackUrl(
            amdStatusCallbackBaseUrl,
            context,
            payload,
            `mode=amd&problemId=${encodeURIComponent(problemId)}&level=${nextLevel}&attempt=${nextAttempt}&levelIndex=${nextIndex}&to=${encodeURIComponent(toNumber)}`
        );
        const ackActionUrl = buildAckActionUrl({
            functionBaseUrl,
            problemId,
            level: nextLevel,
            attempt: nextAttempt,
            levelIndex: nextIndex,
            toNumber,
            replayCount: 0
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
            statusCallback: `${functionBaseUrl}?mode=callback&problemId=${encodeURIComponent(problemId)}&level=${nextLevel}&attempt=${nextAttempt}&levelIndex=${nextIndex}`,
            statusCallbackMethod: 'POST',
            statusCallbackEvent: ['completed']
        });

        await postMonitoringEvent({
            monitoringBackendUrls,
            ingestToken: monitoringIngestToken,
            eventType: 'call_initiated',
            problemId,
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

        return call;
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
        const safeSmsMessage = truncateText(smsMessage, 150);
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

    const tryDial = async (toNumber, nextLevel, nextAttempt, nextIndex) => {
        try {
            await dial(toNumber, nextLevel, nextAttempt, nextIndex, {
                smsSent: false,
                teamsSent: false
            });
            return true;
        } catch (error) {
            console.log(`Call error (non-blocking): ${error.message}`);
            await postMonitoringEvent({
                monitoringBackendUrls,
                ingestToken: monitoringIngestToken,
                eventType: 'call_failed',
                problemId,
                payload: {
                    specialistPhone: toNumber,
                    callStatus: 'failed',
                    level: nextLevel,
                    attempt: nextAttempt,
                    levelIndex: nextIndex,
                    error: truncateText(error.message || 'unknown error', 260)
                }
            });
            return false;
        }
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

            if (digit === '1') {
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

            const unansweredTwiml = buildSimpleTwiml({
                message: 'No recibimos una confirmación válida. La llamada será considerada como no atendida.',
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
                sourceStatus: firstNonEmpty(payload.status, payload.State, payload.state, payload.problemStatus, payload['event.status']) || 'OPEN'
            }
        });

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
                timeoutMs: 2500,
                retries: 0
            });

            if (appIngestResult.delivered) {
                console.log(`Incident ${problemId} created in monitoring app before call flow`);
            }
        } catch (appError) {
            console.log(`Monitoring app webhook create error (non-blocking): ${appError.message}`);
        }

        const callSent = await tryDial(toNumber, effectiveInitialLevel, 1, 0);

        const smsSent = await trySendSms(toNumber, effectiveInitialLevel, 1, 'initiated');
        const teamsSent = await trySendTeamsNotification();

        console.log('Channel status summary:', JSON.stringify({
            problemId,
            phoneSent: callSent,
            smsSent,
            teamsSent
        }));
        callback(null, 'Call initiated');
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


