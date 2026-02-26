import type { Config } from 'tailwindcss';

/**
 * Tailwind CSS Configuration
 *
 * Colors reference CSS variables set in globals.css.
 * Based on template theming system.
 */
const config: Config = {
  darkMode: ['class'],
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      // ============================================================
      // COLORS - Map to CSS variables
      // ============================================================
      colors: {
        // Core colors
        border: 'var(--color-border)',
        ring: 'var(--color-ring)',
        background: 'var(--color-background)',
        foreground: 'var(--color-foreground)',

        // Primary (accent color for buttons, links)
        primary: {
          DEFAULT: 'var(--color-primary)',
          foreground: 'var(--color-primary-foreground)',
        },

        // Secondary (for secondary buttons)
        secondary: {
          DEFAULT: 'var(--color-secondary)',
          foreground: 'var(--color-secondary-foreground)',
        },

        // Destructive (error states)
        destructive: {
          DEFAULT: 'var(--color-destructive)',
          foreground: '#ffffff',
        },

        // Muted (subtle backgrounds)
        muted: {
          DEFAULT: 'var(--color-muted)',
          foreground: 'var(--color-muted-foreground)',
        },

        // Accent (hover states)
        accent: {
          DEFAULT: 'var(--color-accent)',
          foreground: 'var(--color-accent-foreground)',
        },

        // Card/Popover
        card: {
          DEFAULT: 'var(--color-background)',
          foreground: 'var(--color-foreground)',
        },
        popover: {
          DEFAULT: 'var(--color-background)',
          foreground: 'var(--color-foreground)',
        },
      },

      // ============================================================
      // FONTS - Map to CSS variables
      // ============================================================
      fontFamily: {
        sans: 'var(--font-body)',
        heading: 'var(--font-heading)',
        mono: 'var(--font-mono)',
      },

      // ============================================================
      // LAYOUT
      // ============================================================
      borderRadius: {
        sm: 'var(--radius-sm)',
        md: 'var(--radius-md)',
        lg: 'var(--radius-lg)',
        xl: 'var(--radius-xl)',
      },
      spacing: {
        sidebar: 'var(--sidebar-width)',
        header: 'var(--header-height)',
      },
    },
  },
  plugins: [],
};

export default config;
