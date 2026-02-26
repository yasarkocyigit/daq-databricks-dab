import { useState } from 'react';
import {
  Plus,
  MessageSquare,
  Trash2,
  ChevronLeft,
  ChevronRight,
  Loader2,
  BookOpen,
} from 'lucide-react';
import type { Conversation } from '@/lib/types';

interface SidebarProps {
  conversations: Conversation[];
  currentConversationId?: string;
  onConversationSelect: (conversationId: string) => void;
  onNewConversation: () => void;
  onDeleteConversation: (conversationId: string) => void;
  onViewSkills?: () => void;
  isLoading?: boolean;
}

export function Sidebar({
  conversations,
  currentConversationId,
  onConversationSelect,
  onNewConversation,
  onDeleteConversation,
  onViewSkills,
  isLoading = false,
}: SidebarProps) {
  const [hoveredConversation, setHoveredConversation] = useState<string | null>(null);
  const [isCollapsed, setIsCollapsed] = useState(false);

  const handleDelete = (e: React.MouseEvent, conversationId: string) => {
    e.stopPropagation();
    if (confirm('Delete this conversation?')) {
      onDeleteConversation(conversationId);
    }
  };

  return (
    <aside
      className={`
        flex flex-col bg-[var(--color-bg-secondary)] border-r border-[var(--color-border)] shadow-sm h-full relative transition-all duration-300 flex-shrink-0
        ${isCollapsed ? 'w-20' : 'w-[var(--sidebar-width)]'}
      `}
    >
      {/* Header - New Conversation Button */}
      <div
        className={`${isCollapsed ? 'p-2' : 'p-4'} border-b border-[var(--color-border)]/20 transition-all duration-300`}
      >
        <button
          onClick={onNewConversation}
          className={`flex items-center w-full hover:bg-[var(--color-accent-primary)]/[0.08] rounded-xl transition-all duration-300 group ${isCollapsed ? 'p-2 justify-center' : 'p-3 gap-3'}`}
        >
          <div className="flex items-center justify-center rounded-xl bg-[var(--color-accent-primary)] text-white transition-all duration-300 shadow-sm group-hover:shadow-md group-hover:scale-105 h-10 w-10 flex-shrink-0">
            <Plus className="h-5 w-5" strokeWidth={2.5} />
          </div>
          {!isCollapsed && (
            <span className="text-[var(--color-text-heading)] font-semibold text-[15px] transition-opacity duration-300">
              New Chat
            </span>
          )}
        </button>
      </div>

      {/* Conversations List */}
      {!isCollapsed && (
        <div className="flex-1 overflow-y-auto overflow-x-hidden p-3">
          {isLoading ? (
            <div className="text-center py-12 text-[var(--color-text-muted)]">
              <Loader2 className="h-8 w-8 mx-auto mb-3 opacity-60 animate-spin" />
              <p className="text-sm font-medium">Loading...</p>
            </div>
          ) : conversations.length === 0 ? (
            <div className="text-center py-12 text-[var(--color-text-muted)]">
              <MessageSquare className="h-12 w-12 mx-auto mb-3 opacity-40" />
              <p className="text-sm font-medium">No conversations yet</p>
              <p className="text-xs mt-1 opacity-70">
                Start a new conversation!
              </p>
            </div>
          ) : (
            <div className="space-y-1">
              {conversations.map((conv) => (
                <div
                  key={conv.id}
                  onClick={() => onConversationSelect(conv.id)}
                  onMouseEnter={() => setHoveredConversation(conv.id)}
                  onMouseLeave={() => setHoveredConversation(null)}
                  className={`
                    group relative px-2.5 py-2 rounded-lg cursor-pointer transition-all duration-200
                    ${
                      currentConversationId === conv.id
                        ? 'bg-[var(--color-accent-primary)] text-white shadow-sm'
                        : 'bg-[var(--color-background)] hover:bg-[var(--color-background)]/80 text-[var(--color-foreground)] border border-[var(--color-border)]/50 hover:border-[var(--color-border)] hover:shadow-sm'
                    }
                  `}
                >
                  <h3
                    className={`font-medium text-xs truncate pr-6 ${
                      currentConversationId === conv.id
                        ? 'text-white'
                        : 'text-[var(--color-text-heading)]'
                    }`}
                  >
                    {conv.title}
                  </h3>

                  {/* Delete button - show on hover */}
                  {hoveredConversation === conv.id && (
                    <div className="absolute right-2 top-1/2 -translate-y-1/2 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={(e) => handleDelete(e, conv.id)}
                        className="p-1 rounded hover:bg-black/10 transition-colors"
                        title="Delete"
                      >
                        <Trash2
                          className={`h-3 w-3 ${
                            currentConversationId === conv.id
                              ? 'text-white/70 hover:text-white'
                              : 'text-[var(--color-text-muted)] hover:text-[var(--color-error)]'
                          }`}
                        />
                      </button>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* View Skills Button */}
      {!isCollapsed && onViewSkills && (
        <div className="p-3 border-t border-[var(--color-border)]/20">
          <button
            onClick={onViewSkills}
            className="flex items-center gap-2 w-full px-3 py-2 rounded-lg text-xs text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-background)] transition-colors"
          >
            <BookOpen className="h-3.5 w-3.5" />
            View system prompt & skills
          </button>
        </div>
      )}

      {/* Collapse/Expand Button */}
      <div className="absolute -right-3 top-20 z-10">
        <button
          onClick={() => setIsCollapsed(!isCollapsed)}
          className="p-1.5 rounded-full bg-[var(--color-background)] border border-[var(--color-border)] shadow-sm hover:shadow-md transition-all duration-200 group"
          title={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {isCollapsed ? (
            <ChevronRight className="h-3 w-3 text-[var(--color-muted-foreground)] group-hover:text-[var(--color-foreground)]" />
          ) : (
            <ChevronLeft className="h-3 w-3 text-[var(--color-muted-foreground)] group-hover:text-[var(--color-foreground)]" />
          )}
        </button>
      </div>
    </aside>
  );
}
