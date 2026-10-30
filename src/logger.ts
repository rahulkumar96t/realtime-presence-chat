export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';
export type LogFields = Record<string, unknown>;

export interface Logger {
  debug(msg: string, fields?: LogFields): void;
  info(msg: string, fields?: LogFields): void;
  warn(msg: string, fields?: LogFields): void;
  error(msg: string, fields?: LogFields): void;
}

const ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

/** One JSON object per line on stdout; enough for container log collectors. */
export function createLogger(level: LogLevel = 'info', base: LogFields = {}): Logger {
  const write = (at: Exclude<LogLevel, 'silent'>, msg: string, fields: LogFields = {}) => {
    if (ORDER[at] < ORDER[level]) return;
    const record: LogFields = { time: new Date().toISOString(), level: at, msg, ...base };
    for (const [key, value] of Object.entries(fields)) {
      record[key] = value instanceof Error ? { name: value.name, message: value.message } : value;
    }
    process.stdout.write(`${JSON.stringify(record)}\n`);
  };
  return {
    debug: (msg, fields) => write('debug', msg, fields),
    info: (msg, fields) => write('info', msg, fields),
    warn: (msg, fields) => write('warn', msg, fields),
    error: (msg, fields) => write('error', msg, fields),
  };
}

export const silentLogger: Logger = createLogger('silent');
