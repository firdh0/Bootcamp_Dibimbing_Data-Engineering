import React, { useState } from 'react';
import './App.css'; 
import Login from './components/Login';
import Dashboard from './components/Dashboard';


/**
 * The main application component.
 * It manages user authentication state and conditionally renders either the
 * Login page or the main Dashboard.
 * @returns {JSX.Element} The rendered App component.
 */
function App() {

  
  /**
   * @description State to track whether the user is authenticated.
   * @type {[boolean, React.Dispatch<React.SetStateAction<boolean>>]}
   */
  const [isAuthenticated, setIsAuthenticated] = useState(false);


  /**
   * Handles the login attempt by validating user credentials.
   * NOTE: This uses hardcoded credentials for demonstration purposes only.
   * @param {string} username - The username entered by the user.
   * @param {string} password - The password entered by the user.
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
   * Handles the logout process by resetting the authentication state.
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