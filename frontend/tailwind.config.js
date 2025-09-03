/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    safelist: [
        {
            pattern:
                /(bg|text|border)-(red|blue|green|emerald|gray|yellow|purple|pink|indigo|teal|orange|lime|cyan|rose)-(100|200|300|400|500|600|700|800|900)/,
        },
        { pattern: /(bg|text|border)-(white|black)/ },
    ],
    theme: {
        extend: {},
    },
    plugins: [],
}
