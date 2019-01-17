structure BerkeleyDB =
struct

local
  open Foreign
in
  type db = Memory.voidStar
end

end
