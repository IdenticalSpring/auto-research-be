import 'reflect-metadata';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { SearchPapersDto } from './search-papers.dto';

/** AI contract 2026-11-01: country_filter là string enum both|in|out. */
async function validateDto(payload: Record<string, unknown>) {
  const dto = plainToInstance(SearchPapersDto, payload);
  return { dto, errors: await validate(dto as object) };
}

const BASE = {
  documentId: 'doc-1',
  field: 'Kinh tế',
  query: 'digital transformation',
};

describe('SearchPapersDto.countryFilter', () => {
  it.each(['both', 'in', 'out'])('chấp nhận %s', async (value) => {
    const { errors } = await validateDto({ ...BASE, countryFilter: value });
    expect(errors).toHaveLength(0);
  });

  it('mặc định là both khi không truyền', async () => {
    const { dto, errors } = await validateDto(BASE);
    expect(errors).toHaveLength(0);
    expect(dto.countryFilter).toBe('both');
  });

  it('từ chối list (BE không được gửi/gửi list theo contract AI)', async () => {
    const { errors } = await validateDto({
      ...BASE,
      countryFilter: ['in'],
    });
    expect(errors.some((e) => e.property === 'countryFilter')).toBe(true);
  });

  it('từ chối giá trị cũ "vietnam"', async () => {
    const { errors } = await validateDto({
      ...BASE,
      countryFilter: 'vietnam',
    });
    expect(errors.some((e) => e.property === 'countryFilter')).toBe(true);
  });
});

describe('SearchPapersDto.searchRange (AI Q2)', () => {
  it.each(['', 'all'])('chấp nhận %p', async (value) => {
    const { dto, errors } = await validateDto({ ...BASE, searchRange: value });
    expect(errors).toHaveLength(0);
    expect(dto.searchRange).toBe(value);
  });

  it('mặc định là "" khi không truyền', async () => {
    const { dto, errors } = await validateDto(BASE);
    expect(errors).toHaveLength(0);
    expect(dto.searchRange).toBe('');
  });

  it.each(['recent', '2020-2025', 'ALL '])(
    'từ chối giá trị lạ %p',
    async (value) => {
      const { errors } = await validateDto({ ...BASE, searchRange: value });
      expect(errors.some((e) => e.property === 'searchRange')).toBe(true);
    },
  );
});
