import Sidebar from './Sidebar'

interface LayoutProps {
  children: React.ReactNode
}

export default function Layout({ children }: LayoutProps) {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 ml-[220px] p-6">
        {children}
      </main>
    </div>
  )
}
