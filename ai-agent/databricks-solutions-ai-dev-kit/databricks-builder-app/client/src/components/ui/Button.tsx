import { forwardRef, type ButtonHTMLAttributes } from 'react';
import { cn } from '@/lib/utils';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'default' | 'secondary' | 'ghost' | 'destructive' | 'outline';
  size?: 'default' | 'sm' | 'lg' | 'icon';
}

const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'default', size = 'default', ...props }, ref) => {
    return (
      <button
        className={cn(
          'inline-flex items-center justify-center whitespace-nowrap rounded-lg font-medium transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]/50 disabled:pointer-events-none disabled:opacity-50',
          // Variants
          variant === 'default' &&
            'bg-[var(--color-accent-primary)] text-white hover:bg-[var(--color-accent-primary)]/90 shadow-sm hover:shadow-md',
          variant === 'secondary' &&
            'bg-[var(--color-bg-secondary)] text-[var(--color-text-primary)] border border-[var(--color-border)] hover:bg-[var(--color-bg-tertiary)]',
          variant === 'ghost' &&
            'hover:bg-[var(--color-bg-secondary)] text-[var(--color-text-primary)]',
          variant === 'destructive' &&
            'bg-[var(--color-error)] text-white hover:bg-[var(--color-error)]/90',
          variant === 'outline' &&
            'border border-[var(--color-border)] bg-[var(--color-background)] text-[var(--color-text-primary)] hover:bg-[var(--color-bg-secondary)]',
          // Sizes
          size === 'default' && 'h-10 px-4 py-2 text-sm',
          size === 'sm' && 'h-9 px-3 text-xs',
          size === 'lg' && 'h-11 px-8 text-base',
          size === 'icon' && 'h-10 w-10',
          className
        )}
        ref={ref}
        {...props}
      />
    );
  }
);
Button.displayName = 'Button';

export { Button };
