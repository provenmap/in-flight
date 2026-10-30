// The shadcn/ui button, with the variants this view needs.
import { cva, type VariantProps } from 'class-variance-authority';
import type { ComponentProps } from 'react';
import { cn } from './cn';

const button = cva(
  'cursor-pointer rounded-lg border px-3 py-[7px] text-ink transition-[background,border-color,opacity] duration-150 ' +
    'disabled:cursor-default disabled:opacity-45 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
  {
    variants: {
      variant: {
        default: 'border-accent/35 bg-accent/12 enabled:hover:bg-accent/22',
        quiet: 'border-panel-edge bg-transparent text-muted enabled:hover:bg-accent/12',
        on: 'border-accent bg-accent/30',
      },
      size: { default: '', sm: 'px-2.5 py-1 text-xs' },
      suggested: { true: 'border-gold animate-nudge motion-reduce:animate-none', false: '' },
    },
    defaultVariants: { variant: 'default', size: 'default', suggested: false },
  },
);

export function Button({ className, variant, size, suggested, ...props }: ComponentProps<'button'> & VariantProps<typeof button>) {
  return <button type="button" className={cn(button({ variant, size, suggested }), className)} {...props} />;
}
