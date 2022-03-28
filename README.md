## example for byteplus

#### How to run example
Take the retail industry as an example:
* clone the project.
* enter the example directory.
* fill necessary parameters.
* build the binary executable file.
* run executable file.

```shell
git clone https://github.com/byteplus-sdk/example-go.git
cd example-go
go mod tidy
cd retailv2
# fill in tenant, token, tenantID and other parameters.
go build
./retailv2
```