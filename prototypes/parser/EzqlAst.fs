#light

type prog = Prog of clss list

and clss  = Class of string * mthd list

and mthd  = Method of string * id list * expr list

and expr  =
  | MethodCall of expr * id * expr list
  | Lambda of id list * expr list
  | Integer of int
  | Symbol of string
  | Id of id
  | Assign of string * expr

and id = Identifier of string

