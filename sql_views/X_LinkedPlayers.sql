CREATE VIEW "X_LinkedPlayers" AS
select * from LinkedPlayers
union
select * from LinkedPlayers_Rivals
union
select * from LinkedPlayers_NFL
union
select * from LinkedPlayers_AC
union
select * from LinkedPlayers_NCAA
union
select * from LinkedPlayers_AA;