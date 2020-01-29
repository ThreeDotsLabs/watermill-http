module github.com/ThreeDotsLabs/watermill-http

require (
	github.com/ThreeDotsLabs/watermill v1.1.0
	github.com/go-chi/chi v4.0.2+incompatible
	github.com/go-chi/render v1.0.1
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.3.0
)

replace github.com/go-chi/render => github.com/m110/render v1.0.2-0.20200129133022-b0bf9bbedcb9

go 1.13
