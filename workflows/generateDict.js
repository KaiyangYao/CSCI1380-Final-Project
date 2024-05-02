const fs = require('fs').promises;
const path = require('path');

async function listFilesRecursively(dir, outputFile) {
  try {
    const entries = await fs.readdir(dir, {withFileTypes: true});
    for (const entry of entries) {
      const entryPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await listFilesRecursively(entryPath, outputFile);
      } else {
        if (entry.name !== '.DS_Store') {
          await fs.appendFile(outputFile, entry.name + '\n');
        }
      }
    }
  } catch (err) {
    console.error('Error reading directory:', err);
  }
}

const folderPath = path.join(__dirname, '../store');
const outputFilePath = path.join(__dirname, 'dictionary.txt');

listFilesRecursively(folderPath, outputFilePath)
    .then(() => console.log('All file names have been written to dictionary.txt'))
    .catch((err) => console.error('Error during file processing:', err));
