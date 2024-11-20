package models

type Estudiante struct {
	IDEstudiante    int    `json:"idEstudiante"`
	Nombre          string `json:"nombre"`
	Direccion       string `json:"direccion"`
	Email           string `json:"email"`
	Telefono        string `json:"telefono"`
	ClaveEstudiante string `json:"claveEstudiante"`
	AltaLocal       bool   `json:"altaLocal"`
	AltaSep         bool   `json:"altaSep"`
}
