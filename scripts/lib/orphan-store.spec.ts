import { OrphanResponseStore } from './orphan-store';

describe('OrphanResponseStore', () => {
  it('lưu và lấy lại response đến sớm theo document_id + event_type', () => {
    const store = new OrphanResponseStore();
    const data = { document_id: 'd1', event_type: 'search_papers', ok: true };

    const key = store.save('d1', 'search_papers', data);

    expect(key).toBe('d1:search_papers');
    expect(store.size()).toBe(1);
    expect(store.take('d1', 'search_papers')).toBe(data);
    expect(store.size()).toBe(0);
    expect(store.take('d1', 'search_papers')).toBeUndefined();
  });

  it('hết TTL thì không lấy được nữa', () => {
    const store = new OrphanResponseStore(50, 1000);
    store.save('d2', 'ai_in_doc', { ok: true }, 1_000);

    expect(store.take('d2', 'ai_in_doc', 1_500)).toBeDefined();

    store.save('d3', 'ai_in_doc', { ok: true }, 2_000);
    expect(store.take('d3', 'ai_in_doc', 5_000)).toBeUndefined();
  });

  it('vượt số lượng tối đa thì loại entry cũ nhất', () => {
    const store = new OrphanResponseStore(2, 60_000);
    store.save('d1', 'search_papers', { n: 1 }, 1_000);
    store.save('d2', 'search_papers', { n: 2 }, 2_000);
    store.save('d3', 'search_papers', { n: 3 }, 3_000);

    expect(store.size()).toBe(2);
    expect(store.take('d1', 'search_papers', 4_000)).toBeUndefined();
    expect(store.take('d2', 'search_papers', 4_000)).toEqual({ n: 2 });
  });
});
