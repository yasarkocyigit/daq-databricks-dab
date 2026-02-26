import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Folder, Trash2, Loader2, MessageSquare } from 'lucide-react';
import { toast } from 'sonner';
import { MainLayout } from '@/components/layout/MainLayout';
import { Button } from '@/components/ui/Button';
import { Input } from '@/components/ui/Input';
import { useProjects } from '@/contexts/ProjectsContext';
import { useUser } from '@/contexts/UserContext';
import { formatRelativeTime } from '@/lib/utils';

export default function HomePage() {
  const navigate = useNavigate();
  const { loading: userLoading } = useUser();
  const { projects, loading: projectsLoading, createProject, deleteProject } = useProjects();
  const [newProjectName, setNewProjectName] = useState('');
  const [isCreating, setIsCreating] = useState(false);

  const handleCreateProject = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newProjectName.trim()) return;

    setIsCreating(true);
    try {
      const project = await createProject(newProjectName.trim());
      setNewProjectName('');
      toast.success('Project created');
      navigate(`/projects/${project.id}`);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      toast.error(`Failed to create project: ${detail}`);
      console.error(error);
    } finally {
      setIsCreating(false);
    }
  };

  const handleDeleteProject = async (e: React.MouseEvent, projectId: string) => {
    e.stopPropagation();
    if (!confirm('Delete this project and all its conversations?')) return;

    try {
      await deleteProject(projectId);
      toast.success('Project deleted');
    } catch (error) {
      toast.error('Failed to delete project');
      console.error(error);
    }
  };

  if (userLoading || projectsLoading) {
    return (
      <MainLayout>
        <div className="flex h-full items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-[var(--color-text-muted)]" />
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout>
      <div className="flex-1 overflow-hidden flex flex-col">
        <div className="mx-auto max-w-4xl w-full px-4 py-8 flex flex-col flex-1 min-h-0">
          {/* Page Header */}
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-[var(--color-text-heading)]">
              Your Projects
            </h2>
            <p className="mt-1 text-sm text-[var(--color-text-muted)]">
              Create and manage your Databricks AI Dev Kit projects
            </p>
          </div>

          {/* Create Project Form - Always visible */}
          <div className="mb-6 rounded-xl border border-[var(--color-border)] bg-[var(--color-bg-secondary)] p-4 shadow-sm">
            <form onSubmit={handleCreateProject} className="flex gap-3">
              <Input
                value={newProjectName}
                onChange={(e) => setNewProjectName(e.target.value)}
                placeholder="New project name..."
                className="flex-1"
              />
              <Button type="submit" disabled={!newProjectName.trim() || isCreating}>
                {isCreating ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  'Create Project'
                )}
              </Button>
            </form>
          </div>

          {/* Projects List */}
          {projects.length === 0 ? (
            <div className="text-center py-12 text-[var(--color-text-muted)]">
              <Folder className="mx-auto h-10 w-10 opacity-50" />
              <p className="mt-3 text-sm">
                No projects yet. Create one above to get started.
              </p>
            </div>
          ) : (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="grid gap-3 pb-4">
                {projects.map((project) => (
                  <div
                    key={project.id}
                    onClick={() => navigate(`/projects/${project.id}`)}
                    className="group flex cursor-pointer items-center justify-between rounded-xl border border-[var(--color-border)]/50 bg-[var(--color-bg-secondary)] p-4 transition-all duration-200 hover:border-[var(--color-border)] hover:shadow-md"
                  >
                    <div className="flex items-center gap-4">
                      <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-[var(--color-accent-primary)]/10">
                        <Folder className="h-6 w-6 text-[var(--color-accent-primary)]" />
                      </div>
                      <div>
                        <h3 className="font-semibold text-[var(--color-text-heading)]">
                          {project.name}
                        </h3>
                        <div className="mt-1 flex items-center gap-3 text-sm text-[var(--color-text-muted)]">
                          <span className="flex items-center gap-1">
                            <MessageSquare className="h-3.5 w-3.5" />
                            {project.conversation_count} conversation
                            {project.conversation_count !== 1 ? 's' : ''}
                          </span>
                          <span>{formatRelativeTime(project.created_at)}</span>
                        </div>
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="opacity-0 group-hover:opacity-100 transition-opacity"
                      onClick={(e) => handleDeleteProject(e, project.id)}
                    >
                      <Trash2 className="h-4 w-4 text-[var(--color-error)]" />
                    </Button>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </MainLayout>
  );
}
