import { cn } from "@/lib/utils";

// export function Heading({
//   className,
//   children,
// }: {
//   className?: string;
//   children: React.ReactNode;
// }) {
//   return (
//     <h2 className={cn("text-2xl font-semibold tracking-tight", className)}>
//       {children}
//     </h2>
//   );
// }

// export function Subheading({
//   className,
//   children,
// }: {
//   className?: string;
//   children: React.ReactNode;
// }) {
//   return (
//     <h3
//       className={cn(
//         "text-lg font-semibold leading-none tracking-tight",
//         className
//       )}
//     >
//       {children}
//     </h3>
//   );
// }

// export function MutedText({
//   className,
//   children,
// }: {
//   className?: string;
//   children: React.ReactNode;
// }) {
//   return (
//     <p className={cn("text-muted-foreground text-sm", className)}>{children}</p>
//   );
// }

export function StrongText({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <strong className={cn("text-2xl font-semibold tracking-tight", className)}>
      {children}
    </strong>
  );
}

export function SecondaryText({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <p
      className={cn(
        "text-lg font-semibold leading-none tracking-tight",
        className
      )}
    >
      {children}
    </p>
  );
}

export function MutedText({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <p className={cn("text-muted-foreground text-sm", className)}>{children}</p>
  );
}
