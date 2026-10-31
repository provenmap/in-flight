import { ToggleGroup as ToggleGroupPrimitive } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils';

export function ToggleGroup({ className, ...props }: ComponentProps<typeof ToggleGroupPrimitive.Root>) {
  return <ToggleGroupPrimitive.Root data-slot="toggle-group" className={cn('flex w-fit items-center rounded-md border shadow-xs', className)} {...props} />;
}

export function ToggleGroupItem({ className, ...props }: ComponentProps<typeof ToggleGroupPrimitive.Item>) {
  return (
    <ToggleGroupPrimitive.Item
      data-slot="toggle-group-item"
      className={cn(
        'inline-flex h-7 min-w-9 items-center justify-center px-2.5 text-xs font-medium text-muted-foreground transition-colors first:rounded-l-md last:rounded-r-md hover:bg-muted hover:text-foreground focus-visible:z-10 focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none data-[state=on]:bg-accent data-[state=on]:text-accent-foreground',
        className,
      )}
      {...props}
    />
  );
}
