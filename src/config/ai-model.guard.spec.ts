import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

// jest luôn chạy với rootDir là thư mục gốc dự án
const SRC_DIR = join(process.cwd(), 'src');
const ALLOWED_FILE = 'ai-model.config.ts';
const FORBIDDEN_LITERALS = ['gemini-3.5-flash-lite', 'deepseek-flash'];

function collectTsFiles(dir: string): string[] {
  const entries = readdirSync(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectTsFiles(fullPath));
      continue;
    }
    if (!entry.name.endsWith('.ts')) {
      continue;
    }
    if (entry.name.endsWith('.spec.ts')) {
      continue;
    }
    if (entry.name === ALLOWED_FILE) {
      continue;
    }
    files.push(fullPath);
  }
  return files;
}

describe('Guard: không hardcode tên model trong src/', () => {
  const files = collectTsFiles(SRC_DIR);

  it('quét được các file nguồn', () => {
    expect(files.length).toBeGreaterThan(10);
  });

  it.each(FORBIDDEN_LITERALS)(
    'không có literal "%s" ngoài ai-model.config.ts',
    (literal) => {
      const offenders = files.filter((file) =>
        readFileSync(file, 'utf8').includes(literal),
      );

      expect(offenders).toEqual([]);
    },
  );
});
