export const normalizeSearchInput = (value: string): string =>
  value.replace(/[０-９]/g, (digit) => String.fromCharCode(digit.charCodeAt(0) - 0xfee0));
