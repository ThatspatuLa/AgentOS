/// <reference types="vite/client" />

declare module '*.json' {
  const value: any
  export default value
}

declare module '*.css' {
  const content: string
  export default content
}
