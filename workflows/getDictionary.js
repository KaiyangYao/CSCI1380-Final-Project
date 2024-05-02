const fs = require('fs').promises;
const path = require('path');

async function listFilesRecursively(dir, outputFile) {
    try {
        const entries = await fs.readdir(dir, { withFileTypes: true });
        for (const entry of entries) {
            const entryPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                // If the entry is a directory, recurse into it
                await listFilesRecursively(entryPath, outputFile);
            } else {
                // If the entry is a file, append its path to the dictionary.txt file
                if (entry.name !== '.DS_Store')
                    await fs.appendFile(outputFile, entry.name + '\n');
            }
        }
    } catch (err) {
        console.error('Error reading directory:', err);
    }
}

const folderPath = path.join(__dirname, '../store');
const outputFilePath = path.join(__dirname, 'dictionary.txt');

// Start the process and handle errors
listFilesRecursively(folderPath, outputFilePath)
    .then(() => console.log('All file names have been written to dictionary.txt'))
    .catch(err => console.error('Error during file processing:', err));
