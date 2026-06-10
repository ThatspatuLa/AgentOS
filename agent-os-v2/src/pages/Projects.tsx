import { useStore } from '../store'
import { FolderGit2, FileText } from 'lucide-react'

export default function Projects() {
  const { projects } = useStore()

  return (
    <div>
      <h1 className="text-xl font-semibold mb-6">Projects</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {projects.map((project) => (
          <div
            key={project.id}
            className="bg-[#1c2128] border border-[#30363d] rounded-lg p-5 hover:border-[#58a6ff]/40 transition-colors"
          >
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-lg bg-[#58a6ff]/15 flex items-center justify-center">
                <FolderGit2 size={20} className="text-[#58a6ff]" />
              </div>
              <div>
                <div className="font-medium text-[#e6edf3]">{project.label}</div>
                <div className="text-[11px] text-[#484f58]">{project.path}</div>
              </div>
            </div>

            <div className="flex items-center gap-4 text-xs text-[#8b949e]">
              <div className="flex items-center gap-1.5">
                <FileText size={12} />
                <span>{project.file_count} files</span>
              </div>
              <span className={`text-[10px] font-medium uppercase px-2 py-0.5 rounded-full ${
                project.active
                  ? 'bg-[#3fb950]/15 text-[#3fb950]'
                  : 'bg-[#30363d] text-[#8b949e]'
              }`}>
                {project.active ? 'Active' : 'Idle'}
              </span>
            </div>
          </div>
        ))}
      </div>

      {projects.length === 0 && (
        <div className="text-center py-16 text-[#484f58]">
          <FolderGit2 size={40} className="mx-auto mb-3 opacity-50" />
          <p>No projects found.</p>
        </div>
      )}
    </div>
  )
}
