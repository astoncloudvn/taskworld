interface CronField {
  values: Set<number>;
  wildcard: boolean;
}

export interface ParsedCron {
  minute: CronField;
  hour: CronField;
  dayOfMonth: CronField;
  month: CronField;
  dayOfWeek: CronField;
}

/** Parse a standard five-field cron expression (local time, Sunday is 0 or 7). */
export function parseCron(expression: string): ParsedCron {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(
      `Invalid cron expression "${expression}": expected five fields (minute hour day-of-month month day-of-week).`,
    );
  }
  return {
    minute: parseField(parts[0]!, 0, 59, false, expression),
    hour: parseField(parts[1]!, 0, 23, false, expression),
    dayOfMonth: parseField(parts[2]!, 1, 31, false, expression),
    month: parseField(parts[3]!, 1, 12, false, expression),
    dayOfWeek: parseField(parts[4]!, 0, 7, true, expression),
  };
}

export function matchesCron(cron: ParsedCron, date: Date): boolean {
  const minute = date.getMinutes();
  const hour = date.getHours();
  const day = date.getDate();
  const month = date.getMonth() + 1;
  const weekday = date.getDay();

  if (!cron.minute.values.has(minute) || !cron.hour.values.has(hour)) return false;
  if (!cron.month.values.has(month)) return false;

  const dayMatches = cron.dayOfMonth.values.has(day);
  const weekdayMatches = cron.dayOfWeek.values.has(weekday);
  if (cron.dayOfMonth.wildcard && cron.dayOfWeek.wildcard) return true;
  if (cron.dayOfMonth.wildcard) return weekdayMatches;
  if (cron.dayOfWeek.wildcard) return dayMatches;
  return dayMatches || weekdayMatches;
}

function parseField(
  field: string,
  minimum: number,
  maximum: number,
  sunday: boolean,
  expression: string,
): CronField {
  const values = new Set<number>();
  const wildcard = field === "*" || (field.startsWith("*/") && !field.includes(","));
  for (const part of field.split(",")) {
    if (!part) throw invalidCron(expression);
    const [base, stepText, extra] = part.split("/");
    if (extra !== undefined) throw invalidCron(expression);
    const step = stepText === undefined ? 1 : Number(stepText);
    if (!Number.isInteger(step) || step < 1) throw invalidCron(expression);

    let start: number;
    let end: number;
    if (base === "*") {
      start = minimum;
      end = maximum;
    } else if (base?.includes("-")) {
      const range = base.split("-");
      if (range.length !== 2) throw invalidCron(expression);
      start = Number(range[0]);
      end = Number(range[1]);
      if (end < start) throw invalidCron(expression);
    } else {
      start = Number(base);
      end = stepText === undefined ? start : maximum;
    }
    if (
      !Number.isInteger(start) ||
      !Number.isInteger(end) ||
      start < minimum ||
      end > maximum
    ) {
      throw invalidCron(expression);
    }
    for (let value = start; value <= end; value += step) {
      values.add(sunday && value === 7 ? 0 : value);
    }
  }
  if (!values.size) throw invalidCron(expression);
  return { values, wildcard };
}

function invalidCron(expression: string): Error {
  return new Error(`Invalid cron expression "${expression}".`);
}
