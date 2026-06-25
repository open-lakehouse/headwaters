// A dataset's schema, rendered from the read API's `fields` (each an opaque
// google.protobuf.Struct, typically `{ name, type, description? }`).

interface SchemaField {
  name: string;
  type: string;
  description?: string;
}

function toField(raw: unknown): SchemaField {
  const f = (raw ?? {}) as Record<string, unknown>;
  return {
    name: typeof f.name === "string" ? f.name : "",
    type: typeof f.type === "string" ? f.type : "",
    description: typeof f.description === "string" ? f.description : undefined,
  };
}

export interface SchemaTableProps {
  fields: unknown[];
}

export function SchemaTable({ fields }: SchemaTableProps) {
  if (fields.length === 0) {
    return <p className="text-sm text-muted-foreground">No schema recorded.</p>;
  }
  const rows = fields.map(toField);
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-border text-left text-xs uppercase text-muted-foreground">
          <th className="py-1.5 pr-4 font-medium">Column</th>
          <th className="py-1.5 pr-4 font-medium">Type</th>
          <th className="py-1.5 font-medium">Description</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((f) => (
          <tr key={f.name} className="border-b border-border/60">
            <td className="py-1.5 pr-4 font-medium">{f.name}</td>
            <td className="py-1.5 pr-4 font-mono text-xs text-muted-foreground">
              {f.type}
            </td>
            <td className="py-1.5 text-muted-foreground">
              {f.description ?? ""}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
