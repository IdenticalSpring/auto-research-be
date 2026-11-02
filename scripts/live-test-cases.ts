export interface TestCaseDef {
  step: number;
  name: string;
  topic: string;
  timeoutMs: number;
  payload: (ctx: { genOutline: any }) => Record<string, unknown>;
  validate: (res: Record<string, unknown>, ctx: { genOutline: any }) => string;
}

import { CHATBOT_AND_SETUP_CASES } from './live-test-cases-setup';
import { OUTLINE_AND_WRITING_CASES } from './live-test-cases-writing';

export const TEST_CASES: TestCaseDef[] = [
  ...CHATBOT_AND_SETUP_CASES,
  ...OUTLINE_AND_WRITING_CASES,
];
