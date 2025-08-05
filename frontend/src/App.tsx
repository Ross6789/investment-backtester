import { SettingsPage } from "@/pages/settings";
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
} from "react-router-dom";
import { ResultsPage } from "@/pages/results";

// function App() {
//   return (
//     <div>
//       <SettingsPage />
//     </div>
//   );
// }

// export default App;

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Navigate to="/settings" replace />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/results" element={<ResultsPage />} />
      </Routes>
    </Router>
  );
}

export default App;
