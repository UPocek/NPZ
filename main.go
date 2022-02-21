package main

import (
	"Projekat/App"
)

func main() {
	app := App.CreateApp()
	app.RunApp()
	app.StopApp()
}
