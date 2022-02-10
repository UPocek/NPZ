package main

import (
	"Projekat/App"
	"fmt"
)

func main() {
	app := App.CreateApp()

	app.Put("kljuc01", []byte("vrednost1"))
	app.Put("kljuc02", []byte("vrednost2"))
	app.Put("kljuc03", []byte("vrednost3"))
	app.Put("kljuc04", []byte("vrednost4"))
	app.Put("kljuc05", []byte("vrednost5"))
	app.Put("kljuc06", []byte("vrednost6"))
	app.Put("kljuc07", []byte("vrednost7"))
	app.Put("kljuc08", []byte("vrednost8"))
	app.Put("kljuc09", []byte("vrednost9"))
	app.Put("kljuc10", []byte("vrednost10"))
	app.Put("kljuc11", []byte("vrednost11"))
	app.Put("kljuc12", []byte("vrednost12"))
	app.Put("kljuc13", []byte("vrednost13"))
	app.Put("kljuc14", []byte("vrednost14"))
	app.Put("kljuc15", []byte("vrednost15"))
	app.Put("kljuc16", []byte("vrednost16"))
	app.Put("kljuc17", []byte("vrednost17"))
	app.Put("kljuc18", []byte("vrednost18"))
	app.Put("kljuc19", []byte("vrednost19"))
	app.Put("kljuc20", []byte("vrednost20"))
	app.Put("kljuc21", []byte("vrednost21"))
	app.Put("kljuc22", []byte("vrednost22"))
	app.Put("kljuc23", []byte("vrednost23"))
	app.Put("kljuc24", []byte("vrednost24"))
	app.Put("kljuc25", []byte("vrednost25"))
	app.Put("kljuc26", []byte("vrednost26"))
	app.Put("kljuc27", []byte("vrednost27"))
	app.Put("kljuc28", []byte("vrednost28"))
	app.Put("kljuc29", []byte("vrednost29"))
	app.Put("kljuc30", []byte("vrednost30"))
	app.Put("kljuc31", []byte("vrednost31"))
	app.Put("kljuc32", []byte("vrednost32"))
	app.Put("kljuc33", []byte("vrednost33"))
	app.Put("kljuc34", []byte("vrednost34"))
	app.Put("kljuc35", []byte("vrednost35"))
	app.Put("kljuc36", []byte("vrednost36"))

	_, value := app.Get("kljuc34")
	fmt.Println(string(value))
	_, value = app.Get("kljuc34")
	fmt.Println(string(value))

	app.StopApp()
}
