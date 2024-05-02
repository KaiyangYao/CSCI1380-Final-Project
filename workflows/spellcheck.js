const natural = require('natural');
const fs = require('fs');
const path = require('path');

const dictionaryPath = path.join(__dirname, 'dictionary.txt');
const spellcheck = new natural.Spellcheck(fs.readFileSync(dictionaryPath).toString().split('\n'));

function checkSpelling(word) {
  result = [];
  if (!spellcheck.isCorrect(word)) {
    const suggestions = spellcheck.getCorrections(word, 1);
    result = suggestions.slice(0, 3);
  }
  return result;
}

// console.log(checkSpelling('welp'));
module.exports = checkSpelling;

