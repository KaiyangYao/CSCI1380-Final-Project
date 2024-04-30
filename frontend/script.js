
function searchBooks() {
    // const author = document.getElementById('author-search').value.trim();
    // const bookTitle = document.getElementById('book-search').value.trim();
    const input = document.getElementById('search').value.trim();
    // Clear previous results
    var resultsDiv = document.getElementById('results');


    // Dummy search results - replace this part with your actual search logic
    const books = [
        { title: 'Example Book 1', author: 'Author 1', url: 'http://example.com/book1' },
        { title: 'Example Book 2', author: 'Author 2', url: 'http://example.com/book2' }
    ]
    resultsDiv.innerHTML = '';
    let results = []
    if (input === '') {
        resultsDiv.innerHTML = 'Please provide book title or author';
        return;
    } else {
        results = books.filter(b => b.title.toLowerCase().includes(input) || b.author.toLowerCase().includes(input));
    }

    if (results.length === 0) {
        resultsDiv.innerHTML = 'Nothing found, try something else.';
    } else {
        results.forEach(result => {
            const link = document.createElement('a');
            link.href = result.url;
            link.textContent = `${result.title} by ${result.author}`;
            link.target = "_blank";
            resultsDiv.appendChild(link);
            resultsDiv.appendChild(document.createElement('br'));
        });
    }
}

