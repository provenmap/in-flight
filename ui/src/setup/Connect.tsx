// Connect: the saved connection profiles, and a form for a new one. Secrets are never typed here: each is asked for as
// where it lives (an environment variable, a file, the keychain), because a typed secret would be stored in the
// workspace's config.json.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CheckCircle2Icon, Loader2Icon, PlusIcon, Trash2Icon, XCircleIcon } from 'lucide-react';
import { useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Checkbox, Field, Select } from '@/components/ui/field';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { answerOf, api, enc, errorText, isReference, type Presets, type Profile, type Status, type Tested } from '../api';

interface SecretSpec {
  role: string;
  label: string;
  help: string;
  required: boolean;
  /** A username may be plain text; everything else is a reference. */
  plain?: boolean;
}

const truststore: SecretSpec = { role: 'truststorePassword', label: 'Where is the truststore password?', help: 'Only when the truststore has one.', required: false };

/** What each preset asks for. */
const SECRETS: Record<string, SecretSpec[]> = {
  none: [],
  ssl: [truststore],
  'sasl-plain': [
    { role: 'username', label: 'Username', help: 'On Confluent Cloud, the API key. Plain text, or a reference.', required: true, plain: true },
    { role: 'password', label: 'Where is the password?', help: 'On Confluent Cloud, the API secret.', required: true },
  ],
  'scram-sha-256': [
    { role: 'username', label: 'Username', help: 'Plain text, or a reference.', required: true, plain: true },
    { role: 'password', label: 'Where is the password?', help: '', required: true },
    truststore,
  ],
  'scram-sha-512': [
    { role: 'username', label: 'Username', help: 'Plain text, or a reference.', required: true, plain: true },
    { role: 'password', label: 'Where is the password?', help: '', required: true },
    truststore,
  ],
  mtls: [
    { role: 'keystorePassword', label: 'Where is the keystore password?', help: '', required: false },
    { role: 'keyPassword', label: 'Where is the key password?', help: 'Only when the key has its own.', required: false },
    truststore,
  ],
  'msk-iam': [],
};

/** Settings a preset usually needs, offered as rows. */
const SETTINGS: Record<string, string[]> = { ssl: ['ssl.truststore.location'], mtls: ['ssl.keystore.location'] };

const NAME = /^[a-z0-9][a-z0-9._-]{0,62}$/;
const SECRET_KEY = /password|jaas|secret|token/i;

interface Form {
  name: string;
  bootstrap: string;
  auth: string;
  secrets: Record<string, string>;
  settings: { key: string; value: string }[];
  registry: { url: string; username: string; password: string };
  allowWrites: boolean;
}

const blank: Form = { name: 'local', bootstrap: '', auth: 'none', secrets: {}, settings: [], registry: { url: '', username: '', password: '' }, allowWrites: false };

function formOf(p: Profile): Form {
  return {
    name: p.name,
    bootstrap: p.bootstrap,
    auth: p.auth,
    secrets: { ...p.secrets },
    settings: Object.entries(p.settings ?? {}).map(([key, value]) => ({ key, value })),
    registry: { url: p.registry?.url ?? '', username: p.registry?.username ?? '', password: p.registry?.password ?? '' },
    allowWrites: p.allowWrites,
  };
}

function profileOf(f: Form): Profile {
  const secrets: Record<string, string> = {};
  for (const s of SECRETS[f.auth] ?? []) if (f.secrets[s.role]?.trim()) secrets[s.role] = f.secrets[s.role]!.trim();
  const settings: Record<string, string> = {};
  for (const r of f.settings) if (r.key.trim()) settings[r.key.trim()] = r.value.trim();
  const url = f.registry.url.trim();
  return {
    name: f.name.trim(),
    bootstrap: f.bootstrap.trim(),
    auth: f.auth,
    secrets,
    settings,
    ...(url ? { registry: { url, ...(f.registry.username.trim() && { username: f.registry.username.trim() }), ...(f.registry.password.trim() && { password: f.registry.password.trim() }) } } : {}),
    allowWrites: f.allowWrites,
  };
}

const refProblem = (forms: string) =>
  `That is not a reference, so it would be the secret itself, and saved in config.json. Say where it is instead: ${forms}.`;

/** What is wrong with the form, by field, before anything is sent. */
function check(f: Form, forms: string): Record<string, string> {
  const out: Record<string, string> = {};
  if (!NAME.test(f.name.trim())) out.name = 'Lowercase letters, digits, dots, hyphens and underscores, such as staging.';
  if (!f.bootstrap.trim()) out.bootstrap = 'The cluster\'s bootstrap servers, such as kafka-1:9092,kafka-2:9092.';
  for (const s of SECRETS[f.auth] ?? []) {
    const v = f.secrets[s.role]?.trim() ?? '';
    if (!v) {
      if (s.required) out[`secret.${s.role}`] = `${f.auth} needs ${s.plain ? 'a username' : `where the ${s.role.replace(/([A-Z])/g, ' $1').toLowerCase()} is`}.`;
    } else if (!s.plain && !isReference(v)) {
      out[`secret.${s.role}`] = refProblem(forms);
    }
  }
  f.settings.forEach((r, i) => {
    if (r.key.trim() && SECRET_KEY.test(r.key)) out[`setting.${i}`] = 'A secret goes in a reference above, not in settings: settings are saved as they are.';
  });
  if (f.auth === 'mtls' && !f.settings.some(r => r.key.trim() === 'ssl.keystore.location' && r.value.trim())) {
    out.settings = 'Mutual TLS needs ssl.keystore.location, the client\'s keystore.';
  }
  const url = f.registry.url.trim();
  if (url && !/^https?:\/\/.+/.test(url)) out['registry.url'] = 'Such as http://registry:8081.';
  if (f.registry.password.trim() && !isReference(f.registry.password.trim())) out['registry.password'] = refProblem(forms);
  if (!url && (f.registry.username.trim() || f.registry.password.trim())) out['registry.url'] = 'A registry needs its URL.';
  return out;
}

function Result({ result }: { result: Tested }) {
  return (
    <div
      data-testid="connect-result"
      data-ok={result.ok}
      className={cn('flex flex-col gap-2 rounded-md border px-3 py-2.5 text-sm', result.ok ? 'border-recovered/30 bg-recovered/8' : 'border-stuck/30 bg-stuck/8')}
    >
      <p className={cn('flex items-start gap-2 font-medium', result.ok ? 'text-recovered' : 'text-stuck')}>
        {result.ok ? <CheckCircle2Icon className="mt-0.5 size-4 shrink-0" /> : <XCircleIcon className="mt-0.5 size-4 shrink-0" />}
        <span className="whitespace-pre-line">{result.text.split('\n')[0]}</span>
      </p>
      {result.cluster && (
        <p className="text-muted-foreground">
          Cluster <span className="font-mono text-foreground">{result.cluster.id}</span>: {result.cluster.brokers} broker{result.cluster.brokers === 1 ? '' : 's'},{' '}
          {result.cluster.topics} topic{result.cluster.topics === 1 ? '' : 's'}.
        </p>
      )}
      {result.registry && (
        <p className={result.registry.ok ? 'text-muted-foreground' : 'text-stuck'}>
          Schema registry {result.registry.url}: {result.registry.ok ? 'answers.' : result.registry.problem ?? 'does not answer.'}
        </p>
      )}
      {!!result.problems?.length && (
        <ul className="list-disc pl-5 text-stuck">
          {result.problems.map(p => <li key={p}>{p}</li>)}
        </ul>
      )}
    </div>
  );
}

function Profiles({ status, onEdit, onUsed }: { status: Status; onEdit: (p: Profile) => void; onUsed: () => void }) {
  const client = useQueryClient();
  const active = status.profile?.name;
  const act = useMutation({
    mutationFn: ({ name, forget }: { name: string; forget: boolean }) => api<Status>(`/api/profiles/${enc(name)}${forget ? '' : '/use'}`, forget ? 'DELETE' : 'POST'),
    onSuccess: (s, v) => {
      client.setQueryData(['status'], s);
      if (!v.forget) onUsed();
    },
  });
  if (!status.profiles.length) return null;
  return (
    <Card data-testid="profiles">
      <CardHeader>
        <CardTitle>Saved connections</CardTitle>
        <CardDescription>Kept in {status.home ? <span className="font-mono">{status.home}</span> : 'the workspace'}, secrets by reference only.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <ul className="flex flex-col divide-y rounded-md border">
          {status.profiles.map(name => (
            <li key={name} className="flex flex-wrap items-center gap-2 px-3 py-2.5 text-sm">
              <span className="font-medium">{name}</span>
              {name === active && <Badge variant="recovered">In use</Badge>}
              {name === active && status.profile && <span className="truncate text-xs text-muted-foreground">{status.profile.bootstrap}, {status.profile.auth}</span>}
              <span className="ml-auto flex gap-1.5">
                {name === active && status.profile && <Button size="sm" variant="ghost" onClick={() => onEdit(status.profile!)}>Edit</Button>}
                <Button size="sm" variant={name === active ? 'outline' : 'default'} data-testid={`profile-use-${name}`} disabled={act.isPending} onClick={() => act.mutate({ name, forget: false })}>
                  {name === active ? 'Continue' : 'Use'}
                </Button>
                <Button
                  size="sm" variant="ghost" aria-label={`Forget ${name}`} disabled={act.isPending}
                  onClick={() => window.confirm(`Forget the connection ${name}? Its flows stay.`) && act.mutate({ name, forget: true })}
                >
                  <Trash2Icon />
                </Button>
              </span>
            </li>
          ))}
        </ul>
        {act.error && <p className="text-sm text-stuck">{errorText(act.error)}</p>}
      </CardContent>
    </Card>
  );
}

export function Connect({ status, onConnected }: { status: Status; onConnected: () => void }) {
  const client = useQueryClient();
  const presets = useQuery({ queryKey: ['presets'], queryFn: () => api<Presets>('/api/presets'), staleTime: Infinity });
  const forms = presets.data?.secretForms ?? 'env:NAME, file:/path or keychain:service[/account]';
  const [form, setForm] = useState<Form>(() => (status.profiles.length ? { ...blank, name: '' } : blank));
  const [result, setResult] = useState<Tested | null>(null);
  const problems = check(form, forms);
  const ready = Object.keys(problems).length === 0;
  const set = (patch: Partial<Form>) => { setForm(f => ({ ...f, ...patch })); setResult(null); };
  const setSecret = (role: string, v: string) => set({ secrets: { ...form.secrets, [role]: v } });
  const setRegistry = (patch: Partial<Form['registry']>) => set({ registry: { ...form.registry, ...patch } });
  const setAuth = (auth: string) => {
    const wanted = (SETTINGS[auth] ?? []).filter(k => !form.settings.some(r => r.key === k));
    set({ auth, settings: [...form.settings.filter(r => r.key || r.value), ...wanted.map(key => ({ key, value: '' }))] });
  };

  const send = useMutation({
    mutationFn: ({ save }: { save: boolean }) =>
      api<Tested>(save ? '/api/connect' : '/api/test', 'POST', save ? { profile: profileOf(form), use: true } : { profile: profileOf(form) }),
    onSuccess: (r, { save }) => {
      setResult(r);
      if (save && r.ok) {
        void client.invalidateQueries({ queryKey: ['status'] });
        void client.removeQueries({ queryKey: ['discover'] });
        onConnected();
      }
    },
    onError: e => setResult(answerOf<Tested>(e) ?? { ok: false, text: errorText(e) }),
  });

  const help = presets.data?.presets.find(p => p.id === form.auth)?.help;
  const specs = SECRETS[form.auth] ?? [];
  const shown = (key: string, value: string) => (value.trim() ? problems[key] : undefined);

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,380px)]">
      <Card data-testid="connect-form">
        <CardHeader>
          <CardTitle>Connect a cluster</CardTitle>
          <CardDescription>Where your Kafka is and how to sign in to it. Test it first; Save &amp; use keeps it as a profile and makes it the one in use.</CardDescription>
        </CardHeader>
        <CardContent>
          <form className="flex flex-col gap-5" onSubmit={e => { e.preventDefault(); if (ready) send.mutate({ save: true }); }}>
            <div className="grid gap-4 sm:grid-cols-[minmax(0,200px)_minmax(0,1fr)]">
              <Field label="Name" htmlFor="c-name" problem={shown('name', form.name)} help="Such as staging or prod.">
                <Input id="c-name" data-testid="connect-name" value={form.name} onChange={e => set({ name: e.target.value })} placeholder="staging" autoComplete="off" spellCheck={false} aria-invalid={!!shown('name', form.name)} />
              </Field>
              <Field label="Bootstrap servers" htmlFor="c-bootstrap" help="Comma-separated host:port.">
                <Input id="c-bootstrap" data-testid="connect-bootstrap" value={form.bootstrap} onChange={e => set({ bootstrap: e.target.value })} placeholder="localhost:9092" autoComplete="off" spellCheck={false} />
              </Field>
            </div>

            <Field label="Authentication" htmlFor="c-auth" help={help}>
              <Select id="c-auth" data-testid="connect-auth" value={form.auth} onChange={e => setAuth(e.target.value)}>
                {(presets.data?.presets ?? [{ id: 'none', help: '' }]).map(p => <option key={p.id} value={p.id}>{p.id}</option>)}
              </Select>
            </Field>

            {specs.length > 0 && (
              <fieldset className="flex flex-col gap-4 rounded-md border p-4">
                <legend className="px-1 text-sm font-medium">Secrets, by where they are</legend>
                <p className="-mt-1 text-xs text-muted-foreground">
                  Give where each secret lives, never the secret itself: <span className="font-mono">{forms}</span>. It is read when the console connects, and
                  only the reference is saved. A typed password would end up in config.json.
                </p>
                <div className="grid gap-4 sm:grid-cols-2">
                  {specs.map(s => (
                    <Field key={s.role} label={<>{s.label}{!s.required && <span className="font-normal text-muted-foreground"> (optional)</span>}</>} htmlFor={`c-${s.role}`} problem={shown(`secret.${s.role}`, form.secrets[s.role] ?? '')} help={s.help}>
                      <Input
                        id={`c-${s.role}`}
                        data-testid={`connect-secret-${s.role}`}
                        value={form.secrets[s.role] ?? ''}
                        onChange={e => setSecret(s.role, e.target.value)}
                        placeholder={s.plain ? 'my-user or env:KAFKA_USER' : `env:KAFKA_${s.role.replace(/[A-Z]/g, c => `_${c}`).toUpperCase()}`}
                        autoComplete="off"
                        spellCheck={false}
                        aria-invalid={!!shown(`secret.${s.role}`, form.secrets[s.role] ?? '')}
                      />
                    </Field>
                  ))}
                </div>
              </fieldset>
            )}

            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-medium">Client settings <span className="font-normal text-muted-foreground">(optional, not secret)</span></span>
                <Button type="button" size="sm" variant="ghost" onClick={() => set({ settings: [...form.settings, { key: '', value: '' }] })}><PlusIcon />Add</Button>
              </div>
              {form.settings.map((r, i) => (
                <div key={i} className="flex flex-col gap-1">
                  <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] items-center gap-2">
                    <Input
                      aria-label="Setting" value={r.key} placeholder="ssl.truststore.location" spellCheck={false} aria-invalid={!!problems[`setting.${i}`]}
                      onChange={e => set({ settings: form.settings.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)) })}
                    />
                    <Input
                      aria-label="Value" value={r.value} placeholder="/etc/kafka/truststore.p12" spellCheck={false}
                      onChange={e => set({ settings: form.settings.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)) })}
                    />
                    <Button type="button" size="icon" variant="ghost" aria-label="Remove setting" onClick={() => set({ settings: form.settings.filter((_, j) => j !== i) })}><Trash2Icon /></Button>
                  </div>
                  {problems[`setting.${i}`] && <p className="text-xs text-stuck">{problems[`setting.${i}`]}</p>}
                </div>
              ))}
              {problems.settings ? <p className="text-xs text-stuck">{problems.settings}</p> : !form.settings.length && <p className="text-xs text-muted-foreground">Java client settings, such as ssl.truststore.location for a private CA.</p>}
            </div>

            <fieldset className="flex flex-col gap-4 rounded-md border p-4">
              <legend className="px-1 text-sm font-medium">Schema registry <span className="font-normal text-muted-foreground">(optional)</span></legend>
              <p className="-mt-1 text-xs text-muted-foreground">For Avro and Protobuf topics: Confluent&apos;s registry, or Apicurio.</p>
              <Field label="URL" htmlFor="c-reg-url" problem={problems['registry.url']}>
                <Input id="c-reg-url" data-testid="connect-registry" value={form.registry.url} onChange={e => setRegistry({ url: e.target.value })} placeholder="http://registry:8081" spellCheck={false} />
              </Field>
              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Username" htmlFor="c-reg-user">
                  <Input id="c-reg-user" value={form.registry.username} onChange={e => setRegistry({ username: e.target.value })} autoComplete="off" spellCheck={false} />
                </Field>
                <Field label="Where is its password?" htmlFor="c-reg-pass" problem={shown('registry.password', form.registry.password)}>
                  <Input id="c-reg-pass" value={form.registry.password} onChange={e => setRegistry({ password: e.target.value })} placeholder="env:REGISTRY_PASSWORD" autoComplete="off" spellCheck={false} />
                </Field>
              </div>
            </fieldset>

            <label className="flex items-start gap-3 rounded-md border p-4 text-sm">
              <Checkbox className="mt-0.5" data-testid="connect-allow-writes" checked={form.allowWrites} onChange={e => set({ allowWrites: e.target.checked })} />
              <span className="flex flex-col gap-0.5">
                <span className="font-medium">Allow redrive to write to this cluster</span>
                <span className="text-xs text-muted-foreground">
                  Off, the console only reads: it watches, samples and dry-runs. On, a redrive can write dead letters back to the topics they came from,
                  after you confirm. Leave it off for a production cluster until you mean to recover from here.
                </span>
              </span>
            </label>

            <div className="flex flex-wrap items-center gap-2">
              <Button type="button" variant="outline" data-testid="connect-test" disabled={!ready || send.isPending} onClick={() => send.mutate({ save: false })}>
                {send.isPending && !send.variables?.save && <Loader2Icon className="animate-spin" />}Test
              </Button>
              <Button type="submit" data-testid="connect-save" disabled={!ready || send.isPending}>
                {send.isPending && send.variables?.save && <Loader2Icon className="animate-spin" />}Save &amp; use
              </Button>
              {!ready && <span data-testid="connect-missing" className="min-w-0 text-xs text-muted-foreground">First: {Object.values(problems)[0]}</span>}
            </div>
            {result && <Result result={result} />}
          </form>
        </CardContent>
      </Card>
      <div className="flex min-w-0 flex-col gap-4">
        <Profiles status={status} onEdit={p => { setForm(formOf(p)); setResult(null); }} onUsed={onConnected} />
        <Card>
          <CardHeader>
            <CardTitle>What happens next</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 text-sm text-muted-foreground">
            <p>Once connected, the console lists the cluster&apos;s topics, consumer groups and dead-letter queues, and samples the shape of their records.</p>
            <p>You pick the topics of one flow, and it drafts a catalog: which field is the money, the order and the customer. You confirm its guesses, then watch.</p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
