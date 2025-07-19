import React, { useState } from 'react';
import './Login.css'; // Pastikan Anda membuat file Login.css untuk styling

function Login({ onLogin }) {
  // State untuk menyimpan input username dan password
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');

  // Fungsi yang dipanggil saat form disubmit
  const handleSubmit = (event) => {
    event.preventDefault(); // Mencegah refresh halaman default
    onLogin(username, password); // Panggil fungsi onLogin dari parent (App.js)
  };

  return (
    <div className="login-container">
      <h2>Login</h2>
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="username">Username:</label>
          <input
            type="text"
            id="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)} // Update state saat input berubah
            required // Wajib diisi
          />
        </div>
        <div className="form-group">
          <label htmlFor="password">Password:</label>
          <input
            type="password"
            id="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)} // Update state saat input berubah
            required // Wajib diisi
          />
        </div>
        <button type="submit">Login</button>
      </form>
    </div>
  );
}

export default Login;