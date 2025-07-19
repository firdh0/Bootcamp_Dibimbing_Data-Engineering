import React, { useState } from 'react';
import './App.css'; 
import Login from './components/Login';
import Dashboard from './components/Dashboard';


function App() {

  /**
   * Manages the user's authentication state.
   * `isAuthenticated` is `true` if the user is logged in, and `false` otherwise.
   */
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  /**
   * Handles the login process.
   * Checks if the provided username and password match the hardcoded credentials.
   * If they match, it sets `isAuthenticated` to `true` and shows a success alert.
   * If they don't match, it shows a failure alert.
   * username - The username entered by the user.
   * password - The password entered by the user.
   */
  const handleLogin = (username, password) => {
    if (username === 'admin' && password === 'admin123') {
      setIsAuthenticated(true);
      alert('Login Berhasil!');
    } else {
      alert('Username atau Password salah!');
    }
  };

  /**
   * Handles the logout process.
   * Sets `isAuthenticated` to `false` and displays a success alert.
   */
  const handleLogout = () => {
    setIsAuthenticated(false);
    alert('Logout Berhasil!');
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>GoFood Dashboard</h1>
        {isAuthenticated && <button onClick={handleLogout}>Logout</button>}
      </header>
      {isAuthenticated ? (
        <Dashboard />
      ) : (
        <Login onLogin={handleLogin} />
      )}
    </div>
  );
}

export default App;