import { getDatabase } from "@netlify/database";
import { timingSafeEqual } from "node:crypto";

const TABLE = { schema: "public", table: "voz_operacion_casos" };
const MAX_RECORDS_PER_REQUEST = 300;
const MAX_BODY_BYTES = 1_500_000;
const MAX_CENTRAL_PAGE_SIZE = 250;
const MAX_CENTRAL_OFFSET = 100000;

function json(body, status = 200, request = null) {
  const headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Accept, Authorization",
  };
  // Same-site requests need no CORS. Allow only the current Netlify app domain for previews.
  const origin = request?.headers.get("origin") || "";
  if (/^https:\/\/[a-z0-9-]+\.netlify\.app$/i.test(origin) || /^http:\/\/localhost(:\d+)?$/i.test(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Vary"] = "Origin";
  }
  return new Response(status === 204 ? null : JSON.stringify(body), { status, headers });
}

function text(value, max = 4000) {
  if (value == null) return null;
  if (Array.isArray(value)) return value.map((v) => String(v ?? "").trim()).filter(Boolean).join("; ").slice(0, max) || null;
  if (typeof value === "object") return JSON.stringify(value).slice(0, max);
  const result = String(value).trim();
  return result ? result.slice(0, max) : null;
}

function validDate(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function tokenMatches(provided, expected) {
  if (!provided || !expected) return false;
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(expected, "utf8");
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function authorizationError(request) {
  const expected = String(process.env.VOZ_SYNC_TOKEN || "");
  if (expected.length < 20) {
    return { status: 503, error: "Falta configurar la variable privada VOZ_SYNC_TOKEN en Netlify (mínimo 20 caracteres)." };
  }
  const header = String(request.headers.get("authorization") || "");
  const provided = header.startsWith("Bearer ") ? header.slice(7).trim() : "";
  if (!tokenMatches(provided, expected)) {
    return { status: 401, error: "Clave de sincronización incorrecta. Usa la misma clave configurada en Netlify." };
  }
  return null;
}

function normalizeRecord(record) {
  if (!record || typeof record !== "object" || Array.isArray(record)) throw new Error("Cada reporte debe ser un objeto JSON.");
  const caseId = text(record.id || record.case_id || record.caseId, 200);
  if (!caseId) throw new Error("Un reporte no tiene folio/id.");
  const operator = record.operator && typeof record.operator === "object" ? record.operator : {};
  return {
    caseId,
    reportedAt: validDate(record.timestamp || record.response_completed_at),
    operatorName: text(operator.name || record.name, 250),
    area: text(record.area || operator.area, 150),
    shift: text(record.shift || operator.shift, 100),
    experience: text(record.experience || operator.experience, 100),
    problemName: text(record.problem_name, 500),
    problemType: text(record.problem_type, 1000),
    signals: text(record.signals, 1000),
    productionImpact: text(record.production_impact, 500),
    engineeringStatus: text(record.engineering_status, 500),
    frequency: text(record.frequency, 250),
    narrative: text(record.situation_narrative, 12000),
    schema: text(record.schema_version, 100) || text(record.schema, 100) || "voz-operacion-v41.9",
    recordJson: JSON.stringify(record),
  };
}

async function saveOne(db, record) {
  const r = normalizeRecord(record);
  const inserted = await db.sql`
    INSERT INTO ${db.sql.identifier(TABLE)}
      (case_id, reported_at, operator_name, area, shift, experience,
       problem_name, problem_type, signals, production_impact,
       engineering_status, frequency, situation_narrative, source_schema, record)
    VALUES
      (${r.caseId}, ${r.reportedAt}, ${r.operatorName}, ${r.area}, ${r.shift}, ${r.experience},
       ${r.problemName}, ${r.problemType}, ${r.signals}, ${r.productionImpact},
       ${r.engineeringStatus}, ${r.frequency}, ${r.narrative}, ${r.schema}, ${r.recordJson}::jsonb)
    ON CONFLICT (case_id) DO NOTHING
    RETURNING case_id
  `;
  return inserted.length === 1;
}

function parseStoredRecord(value, caseId) {
  let record = value;
  if (typeof record === "string") {
    try { record = JSON.parse(record); } catch { record = null; }
  }
  if (!record || typeof record !== "object" || Array.isArray(record)) return null;
  const result = { ...record };
  if (!result.id) result.id = caseId;
  return result;
}

export default async (request) => {
  if (request.method === "OPTIONS") return json({ ok: true }, 204, request);
  if (!(["GET", "POST"].includes(request.method))) return json({ ok: false, error: "Método no permitido." }, 405, request);

  const authError = authorizationError(request);
  if (authError) return json({ ok: false, error: authError.error }, authError.status, request);

  if (request.method === "GET") {
    try {
      const db = getDatabase();
      const countRows = await db.sql`SELECT COUNT(*)::int AS total FROM ${db.sql.identifier(TABLE)}`;
      const url = new URL(request.url);
      const requestedLimit = Number.parseInt(url.searchParams.get("limit") || String(MAX_CENTRAL_PAGE_SIZE), 10);
      const requestedOffset = Number.parseInt(url.searchParams.get("offset") || "0", 10);
      const limit = Number.isFinite(requestedLimit) ? Math.min(Math.max(requestedLimit, 1), MAX_CENTRAL_PAGE_SIZE) : MAX_CENTRAL_PAGE_SIZE;
      const offset = Number.isFinite(requestedOffset) ? Math.min(Math.max(requestedOffset, 0), MAX_CENTRAL_OFFSET) : 0;
      const rows = await db.sql`
        SELECT row_id, case_id, reported_at, received_at, record
        FROM ${db.sql.identifier(TABLE)}
        ORDER BY row_id ASC
        LIMIT ${limit} OFFSET ${offset}
      `;
      const cases = rows.map((row) => parseStoredRecord(row.record, row.case_id)).filter(Boolean).map((record) => ({ ...record, central_record: true }));
      const total = Number(countRows[0]?.total || 0);
      return json({ ok: true, database: true, table: "voz_operacion_casos", total, offset, limit, returned: cases.length, hasMore: offset + cases.length < total, cases }, 200, request);
    } catch (error) {
      console.error("Error consultando la base central:", error);
      return json({ ok: false, error: "No se pudo consultar la tabla. Verifica que la tabla exista y que Netlify Database esté conectado al sitio." }, 500, request);
    }
  }

  try {
    const declaredLength = Number(request.headers.get("content-length") || 0);
    if (declaredLength > MAX_BODY_BYTES) return json({ ok: false, error: "El envío es demasiado grande. Sincroniza menos reportes por vez." }, 413, request);
    const rawBody = await request.text();
    if (new TextEncoder().encode(rawBody).length > MAX_BODY_BYTES) return json({ ok: false, error: "El envío es demasiado grande. Sincroniza menos reportes por vez." }, 413, request);
    let body;
    try { body = JSON.parse(rawBody); } catch { return json({ ok: false, error: "El contenido recibido no es JSON válido." }, 400, request); }
    const records = body && typeof body === "object" ? (Array.isArray(body.cases) ? body.cases : (body.record ? [body.record] : [])) : [];
    if (!records.length) return json({ ok: false, error: "No se recibieron reportes. Se esperaba 'record' o una lista 'cases'." }, 400, request);
    if (records.length > MAX_RECORDS_PER_REQUEST) return json({ ok: false, error: `Envía como máximo ${MAX_RECORDS_PER_REQUEST} reportes por solicitud.` }, 413, request);
    const db = getDatabase();
    let inserted = 0, duplicates = 0;
    for (const record of records) {
      const didInsert = await saveOne(db, record);
      if (didInsert) inserted += 1; else duplicates += 1;
    }
    return json({ ok: true, inserted, duplicates, received: records.length }, 200, request);
  } catch (error) {
    console.error("No se pudieron guardar los reportes de Voz de la Operación:", error);
    const message = String(error?.message || "");
    if (message.includes("voz_operacion_casos") || message.includes("does not exist") || message.includes("relation")) {
      return json({ ok: false, error: "La tabla central no existe o no es accesible. Ejecuta voz_operacion_schema.sql en la base de datos y vuelve a intentar." }, 500, request);
    }
    if (message.includes("reporte no tiene folio") || message.includes("objeto JSON")) return json({ ok: false, error: message }, 400, request);
    return json({ ok: false, error: "No se pudieron guardar los reportes en PostgreSQL. Revisa los registros de la función en Netlify." }, 500, request);
  }
};
