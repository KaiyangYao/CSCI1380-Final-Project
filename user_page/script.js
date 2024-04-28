function searchBooks() {
    var input = document.getElementById("searchBox").value;
    var resultsDiv = document.getElementById("results");

    // Example static results, replace with actual search logic
    var books = [
        { title: "Book 1", url: "http://example.com/book1" },
        { title: "Book 2", url: "http://example.com/book2" },
        { title: "Book 3", url: "http://example.com/book3" }
    ];

    resultsDiv.innerHTML = ''; // Clear previous results

    // Filter books based on query
    var filteredBooks = books.filter(book => book.title.toLowerCase().includes(input.toLowerCase()));

    if (filteredBooks.length > 0) {
        // Display results
        filteredBooks.forEach(book => {
            var link = document.createElement('a');
            link.href = book.url;
            link.textContent = book.title;
            link.target = "_blank";
            resultsDiv.appendChild(link);
            resultsDiv.appendChild(document.createElement('br'));
        });
    } else {
        resultsDiv.innerHTML = 'Nothing found, try something else.';
    }
}
