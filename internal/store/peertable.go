package store

import (
	"fmt"
	"sync"
)

// PeerKey identifica a un proceso del sistema por su máquina y su ID.
type PeerKey struct {
	MachineID int
	ProcessID int
}

// String representa la clave en formato "M<m>P<p>", útil para logs.
// Entrada: ninguna. Salida: string.
func (k PeerKey) String() string {
	return fmt.Sprintf("M%dP%d", k.MachineID, k.ProcessID)
}

// PeerSnapshot es la copia de solo lectura que este proceso guarda del
// inventario y vetos de OTRO proceso del sistema (propio o de otra
// máquina), recibida vía PushInventory o QueryInventory.
type PeerSnapshot struct {
	Inventory []Item
	Vetos     []VetoEntry
}

// PeerTable mantiene, para este proceso, una copia de solo lectura del
// inventario y vetos de TODOS los demás procesos del sistema (de su misma
// máquina y de las demás). Es thread-safe.
type PeerTable struct {
	mu   sync.RWMutex
	data map[PeerKey]PeerSnapshot
}

// NewPeerTable crea una tabla de peers vacía.
// Entrada: ninguna. Salida: *PeerTable inicializada.
func NewPeerTable() *PeerTable {
	return &PeerTable{data: make(map[PeerKey]PeerSnapshot)}
}

// Update guarda o reemplaza la copia conocida de un proceso específico.
// Entrada: PeerKey del proceso emisor, inventario y vetos reportados.
// Salida: ninguna.
func (t *PeerTable) Update(key PeerKey, inventory []Item, vetos []VetoEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	invCp := make([]Item, len(inventory))
	copy(invCp, inventory)
	vetoCp := make([]VetoEntry, len(vetos))
	copy(vetoCp, vetos)
	t.data[key] = PeerSnapshot{Inventory: invCp, Vetos: vetoCp}
}

// Get devuelve la copia almacenada de un proceso específico, si existe.
// Entrada: PeerKey del proceso consultado. Salida: snapshot y si fue encontrado.
func (t *PeerTable) Get(key PeerKey) (PeerSnapshot, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap, ok := t.data[key]
	return snap, ok
}

// Snapshot devuelve una copia de todas las entradas actuales de la tabla,
// útil para depuración o para mostrar el estado completo conocido.
// Entrada: ninguna. Salida: mapa copia de PeerKey -> PeerSnapshot.
func (t *PeerTable) Snapshot() map[PeerKey]PeerSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[PeerKey]PeerSnapshot, len(t.data))
	for k, v := range t.data {
		out[k] = v
	}
	return out
}
