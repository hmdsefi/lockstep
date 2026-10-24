// Package bad holds directives with unknown names and unknown or unsuppressible IDs.
package bad

/* want `^LS002: unknown directive //lockstep:allowed; known directives: allow, immutable, resource$` */ //lockstep:allowed x y

/* want `^LS002: unknown directive //lockstep:Allow; known directives: allow, immutable, resource$` */ //lockstep:Allow LS100 r

/* want `^LS002: unknown check ID LS999; known: LS100, LS110$` */ //lockstep:allow LS999 r

/* want `^LS002: LS001 cannot be suppressed$` */ //lockstep:allow LS001 r

/* want `^LS002: LS002 cannot be suppressed$` */ //lockstep:allow LS002 r

/* want `^LS002: unknown check ID LS998; known: LS100, LS110$` `^LS002: unknown check ID LS999; known: LS100, LS110$` */ //lockstep:allow LS998,LS999 r

/* want `^LS002: unknown check ID LS230; known: LS100, LS110$` */ //lockstep:allow LS230 r

/* want `^LS001: //lockstep:allow LS999 needs a reason$` `^LS002: unknown check ID LS999; known: LS100, LS110$` */ //lockstep:allow LS999
