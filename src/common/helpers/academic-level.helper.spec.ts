import {
  normalizeAcademicLevel,
  normalizeAiLanguage,
} from './academic-level.helper';

describe('academic-level.helper (BE-BUG-01)', () => {
  describe('normalizeAcademicLevel', () => {
    it('should default to KHOA_LUAN when level is empty or undefined', () => {
      expect(normalizeAcademicLevel()).toBe('KHOA_LUAN');
      expect(normalizeAcademicLevel('')).toBe('KHOA_LUAN');
    });

    it('should return uppercase enum when valid enum is provided', () => {
      expect(normalizeAcademicLevel('CHUYEN_DE')).toBe('CHUYEN_DE');
      expect(normalizeAcademicLevel('luan_an_tien_si')).toBe('LUAN_AN_TIEN_SI');
      expect(normalizeAcademicLevel('khoa_luan')).toBe('KHOA_LUAN');
      expect(normalizeAcademicLevel('tieu_luan')).toBe('TIEU_LUAN');
      expect(normalizeAcademicLevel('bai_bao_khoa_hoc')).toBe('BAI_BAO_KHOA_HOC');
    });

    it('should normalize undergraduate / bachelor / dai hoc terms to KHOA_LUAN', () => {
      expect(normalizeAcademicLevel('dai_hoc')).toBe('KHOA_LUAN');
      expect(normalizeAcademicLevel('đại học')).toBe('KHOA_LUAN');
      expect(normalizeAcademicLevel('cử nhân')).toBe('KHOA_LUAN');
      expect(normalizeAcademicLevel('bachelor thesis')).toBe('KHOA_LUAN');
    });

    it('should normalize master terms to LUAN_VAN_THAC_SI', () => {
      expect(normalizeAcademicLevel('thạc sĩ')).toBe('LUAN_VAN_THAC_SI');
      expect(normalizeAcademicLevel('thac si')).toBe('LUAN_VAN_THAC_SI');
      expect(normalizeAcademicLevel('master')).toBe('LUAN_VAN_THAC_SI');
      expect(normalizeAcademicLevel('luận văn cao học')).toBe('LUAN_VAN_THAC_SI');
    });

    it('should normalize phd / doctoral terms to LUAN_AN_TIEN_SI', () => {
      expect(normalizeAcademicLevel('tiến sĩ')).toBe('LUAN_AN_TIEN_SI');
      expect(normalizeAcademicLevel('tien si')).toBe('LUAN_AN_TIEN_SI');
      expect(normalizeAcademicLevel('phd')).toBe('LUAN_AN_TIEN_SI');
      expect(normalizeAcademicLevel('luận án tiến sĩ')).toBe('LUAN_AN_TIEN_SI');
    });

    it('should normalize essay terms to TIEU_LUAN', () => {
      expect(normalizeAcademicLevel('tiểu luận')).toBe('TIEU_LUAN');
      expect(normalizeAcademicLevel('tieu luan')).toBe('TIEU_LUAN');
    });

    it('should normalize scientific paper terms to BAI_BAO_KHOA_HOC', () => {
      expect(normalizeAcademicLevel('bài báo khoa học')).toBe('BAI_BAO_KHOA_HOC');
      expect(normalizeAcademicLevel('journal paper')).toBe('BAI_BAO_KHOA_HOC');
      expect(normalizeAcademicLevel('nghiên cứu khoa học')).toBe('BAI_BAO_KHOA_HOC');
    });
  });

  describe('normalizeAiLanguage', () => {
    it('should default to tiếng việt when lang is undefined or empty', () => {
      expect(normalizeAiLanguage()).toBe('tiếng việt');
      expect(normalizeAiLanguage('')).toBe('tiếng việt');
    });

    it('should normalize english when en or english is provided', () => {
      expect(normalizeAiLanguage('en')).toBe('english');
      expect(normalizeAiLanguage('english')).toBe('english');
      expect(normalizeAiLanguage('EN')).toBe('english');
    });

    it('should return tiếng việt for vietnamese input', () => {
      expect(normalizeAiLanguage('vi')).toBe('tiếng việt');
      expect(normalizeAiLanguage('vietnamese')).toBe('tiếng việt');
    });
  });
});
