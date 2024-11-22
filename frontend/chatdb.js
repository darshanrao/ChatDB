// script.js
const sendBtn = document.getElementById('send-btn');
const userInput = document.getElementById('user-input');
const messages = document.getElementById('messages');

sendBtn.addEventListener('click', () => {
    const query = userInput.value.trim();
    if (!query) return;

    addMessage(query, 'user-message');
    userInput.value = '';

    // Call Flask backend
    fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
    })
    .then(response => response.json())
    .then(data => {
        addMessage(data.response, 'bot-message');
        displayTable(data.results);
    })
    .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));

});

function addMessage(text, className) {
    const msg = document.createElement('div');
    msg.textContent = text;
    msg.classList.add('message', className);
    messages.appendChild(msg);
    messages.scrollTop = messages.scrollHeight;
}


function displayTable(data) {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '';

  if (data.length === 0) {
    resultsDiv.innerHTML = '<p>No results found.</p>';
    return;
  }

  // Create table
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  // Add table headers
  const headers = Object.keys(data[0]);
  const headerRow = document.createElement('tr');
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  // Add table rows
  data.forEach(row => {
    const tr = document.createElement('tr');
    headers.forEach(header => {
      const td = document.createElement('td');
      td.textContent = row[header];
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
  resultsDiv.appendChild(table);
}