/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: '#0d1117',
        surface: '#161b22',
        border: '#30363d',
        muted: '#7d8590',
        primary: '#e6edf3',
        green: '#3fb950',
        red: '#f85149',
        blue: '#58a6ff',
        yellow: '#d29922',
        purple: '#bc8cff',
        gold: '#e3b341',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'Consolas', 'monospace'],
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
