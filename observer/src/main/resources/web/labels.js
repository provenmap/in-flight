// Label content, shared by both views. A label is { name, lines: [{ text, tone }], error?, alarm? }.

const escape = s => String(s).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[ch]);

export function labelHtml(l) {
  const lines = (l.lines ?? []).map(x => `<div class="line ${x.tone ?? ''}">${escape(x.text)}</div>`).join('');
  const err = l.error ? `<div class="err">${escape(l.error)}</div>` : '';
  return `<div class="name">${escape(l.name)}</div>${lines}${err}`;
}
