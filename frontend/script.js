// eslint-disable-next-line no-unused-vars
function searchBooks(searchType) {
  const searchTerm = document.getElementById('search').value;
  //   console.log('Search term retrieved:', searchTerm);

  if (!searchTerm) {
    document.getElementById('results').innerText = 'Please enter a search term.';
  }

  console.log('Making API request', `term=${encodeURIComponent(searchTerm)}&type=${encodeURIComponent(searchType)}`);
  fetch(`http://localhost:3000/search?term=${encodeURIComponent(searchTerm)}&type=${encodeURIComponent(searchType)}`)
      .then((response) => {
        console.log('API request successful');
        return response.json();
      })
      .then((data) => {
        console.log('Data received from API:', data);
        displayResults(data);
      })
      .catch((error) => {
        console.error('Error during fetch operation:', error);
        document.getElementById('results').innerText = `No ${searchType} found, try another one`;
      });
}


function displayResults(data) {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '';

  if (data.titleResult || data.authorResult) {
    if (data.titleResult) {
      resultsDiv.innerHTML += `<p><strong>Title Match:</strong> ${data.titleResult}</p>`;
    }
    if (data.authorResult) {
      resultsDiv.innerHTML += `<p><strong>Author Match:</strong> ${data.authorResult}</p>`;
    }
  } else {
    resultsDiv.innerHTML = '<p>No results found. Try a different search term.</p>';
  }
}
