// function searchBooks() {
//     var input = document.getElementById("searchBox").value;
//     var resultsDiv = document.getElementById("results");

//     // Example static results, replace with actual search logic
//     var books = [
//         { title: "Book 1", url: "http://example.com/book1" },
//         { title: "Book 2", url: "http://example.com/book2" },
//         { title: "Book 3", url: "http://example.com/book3" }
//     ];

//     resultsDiv.innerHTML = ''; // Clear previous results

//     // Filter books based on query
//     var filteredBooks = books.filter(book => book.title.toLowerCase().includes(input.toLowerCase()));

//     if (filteredBooks.length > 0) {
//         // Display results
//         filteredBooks.forEach(book => {
//             var link = document.createElement('a');
//             link.href = book.url;
//             link.textContent = book.title;
//             link.target = "_blank";
//             resultsDiv.appendChild(link);
//             resultsDiv.appendChild(document.createElement('br'));
//         });
//     } else {
//         resultsDiv.innerHTML = 'Nothing found, try something else.';
//     }
// }
function searchBooks() {
    // const author = document.getElementById('author-search').value.trim();
    // const bookTitle = document.getElementById('book-search').value.trim();
    const input = document.getElementById('search').value.trim();
    // Clear previous results
    document.getElementById('results').innerHTML = '';


    // Dummy search results - replace this part with your actual search logic
    const books = [
        { title: 'Example Book 1', author: 'Author 1', url: 'http://example.com/book1' },
        { title: 'Example Book 2', author: 'Author 2', url: 'http://example.com/book2' }
    ]
    let results = []
    if (input === '') {
        document.getElementById('results').innerHTML = 'Please provide book title or author';
        return;
    } else {
        results = books.filter(b => b.title.toLowerCase().includes(input) || b.author.toLowerCase().includes(input));
    }

    if (results.length === 0) {
        document.getElementById('results').innerHTML = 'Nothing found, try something else.';
    } else {
        const resultList = document.createElement('ul');
        results.forEach(result => {
            const item = document.createElement('li');
            const link = document.createElement('a');
            link.href = result.url;
            link.textContent = `${result.title} by ${result.author}`;
            link.target = "_blank";
            item.appendChild(link);
            resultList.appendChild(item);
        });
        document.getElementById('results').appendChild(resultList);
    }
}

