function map(url, content) {
  const termFrequency = {};
  const words = content.toLowerCase().match(/\w+/g) || [];
  const totalWords = words.length;
  const output = [];

  words.forEach((word) => {
    termFrequency[word] = (termFrequency[word] || 0) + 1;
  });

  for (const [term, count] of Object.entries(termFrequency)) {
    // Normalize term frequency by the total number of words in the document
    const normalizedFrequency = count / totalWords;
    let o = {};
    o[term] = { url, frequency: normalizedFrequency };
    output.push(o);
  }

  return output;
}

function reduce(term, values) {
  let out = {};
  out[term] = { tf: values, idf: values.length };
  return out;
}
