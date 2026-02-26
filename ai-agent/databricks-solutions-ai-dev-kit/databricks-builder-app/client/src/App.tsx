import { Routes, Route, Navigate } from "react-router-dom";
import { Toaster } from "sonner";
import { UserProvider } from "./contexts/UserContext";
import { ProjectsProvider } from "./contexts/ProjectsContext";
import HomePage from "./pages/HomePage";
import ProjectPage from "./pages/ProjectPage";
import DocPage from "./pages/DocPage";

function App() {
  return (
    <UserProvider>
      <ProjectsProvider>
        <div className="min-h-screen bg-background">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/doc" element={<DocPage />} />
            <Route path="/projects/:projectId" element={<ProjectPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
          <Toaster position="bottom-right" />
        </div>
      </ProjectsProvider>
    </UserProvider>
  );
}

export default App;
