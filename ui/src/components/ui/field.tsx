// Plain form controls styled like the Input: a select, a checkbox, a textarea, and a labelled field around any of them.
import type { ComponentProps, ReactNode } from 'react';
import { cn } from '@/lib/utils';

const control =
  'w-full min-w-0 rounded-md border border-input bg-transparent text-base shadow-xs transition-[color,box-shadow] outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50 md:text-sm dark:bg-input/30 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 aria-invalid:border-stuck';

export function Select({ className, ...props }: ComponentProps<'select'>) {
  return <select data-slot="select" className={cn(control, 'h-9 px-2.5 py-1', className)} {...props} />;
}

export function Textarea({ className, ...props }: ComponentProps<'textarea'>) {
  return <textarea data-slot="textarea" className={cn(control, 'min-h-24 px-3 py-2', className)} {...props} />;
}

export function Checkbox({ className, ...props }: Omit<ComponentProps<'input'>, 'type'>) {
  return <input type="checkbox" data-slot="checkbox" className={cn('size-4 shrink-0 cursor-pointer rounded-sm border-input accent-primary disabled:cursor-not-allowed', className)} {...props} />;
}

/** A label, the control, and a line of help or of what is wrong with it. */
export function Field({ label, htmlFor, help, problem, className, children }: { label: ReactNode; htmlFor?: string; help?: ReactNode; problem?: string | null; className?: string; children: ReactNode }) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1.5', className)}>
      <label htmlFor={htmlFor} className="text-sm font-medium">{label}</label>
      {children}
      {problem ? <p className="text-xs text-stuck">{problem}</p> : help ? <p className="text-xs text-muted-foreground">{help}</p> : null}
    </div>
  );
}
