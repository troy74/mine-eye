const PROJECTS_KEY = "mineeye:projects:v1";
const ACTIVE_KEY = "mineeye:activeProjectId";
let storageScope = "guest";

export type StoredProject = {
  /** Client-generated id for UI + chat keys */
  localId: string;
  name: string;
  workspaceId: string;
  graphId: string;
  createdAt: number;
};

function safeParse<T>(raw: string | null, fallback: T): T {
  if (!raw) return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function scopedKey(base: string): string {
  return `${base}:${storageScope}`;
}

export function setProjectStorageScope(scope: string | null) {
  storageScope = scope && scope.trim().length > 0 ? scope.trim() : "guest";
}

export function loadProjects(): StoredProject[] {
  const list = safeParse<StoredProject[]>(localStorage.getItem(scopedKey(PROJECTS_KEY)), []);
  return Array.isArray(list) ? list : [];
}

export function saveProjects(projects: StoredProject[]) {
  localStorage.setItem(scopedKey(PROJECTS_KEY), JSON.stringify(projects));
}

export function getActiveProjectId(): string | null {
  return localStorage.getItem(scopedKey(ACTIVE_KEY));
}

export function setActiveProjectId(localId: string | null) {
  if (localId) localStorage.setItem(scopedKey(ACTIVE_KEY), localId);
  else localStorage.removeItem(scopedKey(ACTIVE_KEY));
}

export function upsertProject(p: StoredProject) {
  const all = loadProjects().filter((x) => x.localId !== p.localId);
  all.push(p);
  all.sort((a, b) => b.createdAt - a.createdAt);
  saveProjects(all);
}

export function findProjectByGraphId(graphId: string): StoredProject | undefined {
  return loadProjects().find((p) => p.graphId === graphId);
}

export function deleteProject(localId: string): void {
  const remaining = loadProjects().filter((p) => p.localId !== localId);
  saveProjects(remaining);
  // Clear active pointer if it was pointing at the deleted project
  if (getActiveProjectId() === localId) setActiveProjectId(null);
}
