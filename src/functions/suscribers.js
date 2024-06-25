const { app } = require("@azure/functions");
const hubspot = require("@hubspot/api-client");
const axios = require("axios").default;

app.timer("suscribers", {
  schedule: '0 */2 * * * *',
  // schedule: "*/30 * * * * *",
  handler: async (request, context) => {
    try {
      const requestConfig = {
        method: "GET",
        url: "https://api.hubapi.com/cms/v3/hubdb/tables/20720399/rows?synced=false",
        headers: {
          Authorization: "Bearer xxxxxxx",
        },
        timeout: 10000,
      };
      const response = await axios(requestConfig);
      const users = response.data.results;

      const usersStage1 = [];
      const usersStage2 = [];
      const usersStage3 = [];

      if (users.length > 0) {
        for (let index = 0; index < users.length; index++) {
          const userData = users[index];

          switch (userData.values.process) {
            case 1:
              usersStage1.push(JSON.parse(userData.values.data));
              break;
            case 2:
              usersStage2.push(JSON.parse(userData.values.data_formated));
              break;
            case 3:
              const dataFormated = [JSON.parse(userData.values.data_formated)];
              const validContacts2 = dataFormated.filter(
                (contact) => contact !== null
              );
              usersStage3.push(
                validContacts2.map(
                  (user) => user.properties.tuboleta_user_id
                )[0]
              );
              break;
          }
        }
      }

      const config_token = {
        method: "POST",
        url: `xxxxxxxxxxxxxxxxxxxxxx`,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          Sandbox: true,
        },
        data: {
          userName: "xxxxx",
          password: "xxxxxxxxxx",
        },
        timeout: 10000,
      };

      const response_token = await axios(config_token);

      const token = response_token.data.token;

      console.log(
        "--------------------- Syncing Subscribers ---------------------"
      );

      const config_suscribers = {
        method: "GET",
        url: `xxxxxxxxxxxxxxxx`,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          Authorization: `Bearer ${token}`,
          Sandbox: true,
        },
        timeout: 10000,
      };

      const response_suscribers = await axios(config_suscribers);

      // const suscribers = response_suscribers.data;

      const suscribers = [
        {
          idUsuario: 1312981892,
          correo: "testcron@hotmail.com",
          telefono: "3188232635",
          identificacion: "1032474715",
          fechaMovimiento: "2023-11-28 00:00:00",
          tipoIdentificacion: "CC",
          nombre: "Michael",
          apellido: "Orejuela",
          genero: "HOMBRE",
          tipoUsuario: "Comprador",
          fechaNacimiento: "1995-09-04",
          ciudad: "Cali",
          departamento: "Departamento",
          pais: "Colombia",
          direccion: "Carrera 25 Sur 48A 14",
          tratamientoDatos: "Teat",
        },
        {
          idUsuario: 2987612987298,
          correo: "testcron2@hotmail.com",
          telefono: "3188232635",
          identificacion: "1032474715",
          fechaMovimiento: "2023-11-28 00:00:00",
          tipoIdentificacion: "CC",
          nombre: "John",
          apellido: "Smith",
          genero: "HOMBRE",
          tipoUsuario: "Comprador",
          fechaNacimiento: "1995-09-04",
          ciudad: "Cali",
          departamento: "Departamento",
          pais: "Colombia",
          direccion: "Carrera 25 Sur 48A 14",
          tratamientoDatos: "Teat",
        },
      ];

      const fusion1 = [];
      const fusion2 = [];
      const fusion3 = [];

      for (const [index, us] of usersStage1.entries()) {
        fusion1.push({
          idUsuario: us.idUsuario,
          correo: us.correo,
          telefono: us.telefono,
          identificacion: us.identificacion,
          fechaMovimiento: us.fechaMovimiento,
          tipoIdentificacion: us.tipoIdentificacion,
          nombre: us.nombre,
          apellido: us.apellido,
          genero: us.genero,
          tipoUsuario: us.tipoUsuario,
          fechaNacimiento: us.fechaNacimiento,
          ciudad: us.ciudad,
          departamento: us.departamento,
          pais: us.pais,
          direccion: us.direccion,
          tratamientoDatos: us.tratamientoDatos,
          retry: true,
        });
      }

      for (const [index, us] of usersStage2.entries()) {
        fusion2.push({
          properties: {
            tuboleta_user_id: us.properties.tuboleta_user_id,
            tuboleta_id_updated: us.properties.tuboleta_id_updated,
            email: us.properties.email,
            firstname: us.properties.firstname,
            lastname: us.properties.lastname,
            phone: us.properties.phone,
            documento_identificacion: us.properties.documento_identificacion,
            fecha_movimiento: us.properties.fecha_movimiento,
            tipo_documento_identificacion:
              us.properties.tipo_documento_identificacion,
            genero: us.properties.genero,
            tipo_usuario: us.properties.tipo_usuario,
            date_of_birth: us.properties.date_of_birth,
            city: us.properties.city,
            state: us.properties.state,
            country: us.properties.country,
            address: us.properties.address,
            tratamiento_datos: us.properties.tratamiento_datos,
            integration: us.properties.integration,
          },
        });
      }

      for (const [index, us] of usersStage3.entries()) {
        fusion3.push(us);
      }

      for (const [index, us] of suscribers.entries()) {
        const found = fusion1.find(
          (element) => element.idUsuario == us.idUsuario
        );
        const found2 = fusion2.find(
          (element) => element.properties.tuboleta_user_id == us.idUsuario
        );
        const found3 = fusion3.find((element) => element == us.idUsuario);

        if (!found?.idUsuario) {
          fusion1.push({
            idUsuario: us.idUsuario,
            correo: us.correo,
            telefono: us.telefono,
            identificacion: us.identificacion,
            fechaMovimiento: us.fechaMovimiento,
            tipoIdentificacion: us.tipoIdentificacion,
            nombre: us.nombre,
            apellido: us.apellido,
            genero: us.genero,
            tipoUsuario: us.tipoUsuario,
            fechaNacimiento: us.fechaNacimiento,
            ciudad: us.ciudad,
            departamento: us.departamento,
            pais: us.pais,
            direccion: us.direccion,
            tratamientoDatos: us.tratamientoDatos,
            retry: found2?.properties || found3 ? true : false,
          });
        }
      }

      const validatedUsers = [];

      console.log(
        "--------------------- Verifying Susbcribers ---------------------"
      );

      for (const user of fusion1) {
        const exist = await validateUser(user);
        if (!user.retry) {
          const data = {
            process: 1,
            user_id: user.idUsuario + "",
            data: JSON.stringify(user),
            data_formated: null,
            synced: exist ? 1 : 0,
          };

          const exist2 = await validateContactHubDB(user.idUsuario);

          if (exist2 == 0) {
            await InsertUserHubDB(data);
            // throw error;
          }
        }
        if (!exist) {
          validatedUsers.push(await buildContact(user));
          await UpdateUserHubDB(await buildContact(user));
          // throw error;
        }
        await delay(5000);
      }

      for (const [index, us] of validatedUsers.entries()) {
        const found = fusion2.find(
          (element) =>
            element.properties.tuboleta_user_id ==
            us.properties.tuboleta_user_id
        );

        if (!found?.properties) {
          fusion2.push({
            properties: {
              tuboleta_user_id: us.properties.tuboleta_user_id,
              tuboleta_id_updated: us.properties.tuboleta_id_updated,
              email: us.properties.email,
              firstname: us.properties.firstname,
              lastname: us.properties.lastname,
              phone: us.properties.phone,
              documento_identificacion: us.properties.documento_identificacion,
              fecha_movimiento: us.properties.fecha_movimiento,
              tipo_documento_identificacion:
                us.properties.tipo_documento_identificacion,
              genero: us.properties.genero,
              tipo_usuario: us.properties.tipo_usuario,
              date_of_birth: us.properties.date_of_birth,
              city: us.properties.city,
              state: us.properties.state,
              country: us.properties.country,
              address: us.properties.address,
              tratamiento_datos: us.properties.tratamiento_datos,
              integration: us.properties.integration,
            },
          });
        }
      }

      const validContacts = fusion2.filter((contact) => contact !== null);

      const suscribersProperties = {
        inputs: validatedUsers,
      };

      const tuboletaUserIds = validContacts.map(
        (user) => user.properties.tuboleta_user_id
      );

      for (const [index, us] of tuboletaUserIds.entries()) {
        const found = fusion3.find((element) => element == us);

        if (!found) {
          fusion3.push(us);
        }
      }

      if (validContacts.length > 0) {
        const config_create_suscribers = {
          method: "POST",
          url: `https://api.hubapi.com/crm/v3/objects/contacts/batch/create`,
          headers: {
            "content-type": "application/json",
            Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
          },
          data: suscribersProperties,
          timeout: 10000,
        };

        await axios(config_create_suscribers);
        console.log(
          "--------------------- Susbcribers added successfully ---------------------"
        );
        //   context.log(`Suscribers added successfully!`);
      } else {
        console.log(
          "--------------------- No Suscribers to add ---------------------"
        );
        //   context.log(`No Suscribers to add`);
      }

      if (fusion3.length > 0) {
        await SyncUserHubDB(fusion3);
      }

      console.log("--------------------- Syncing Sales ---------------------");
      await delay(10000);

      const requestConfigSales = {
        method: "GET",
        url: "https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows?synced=false",
        headers: {
          Authorization: "Bearer xxxxxxxxxxxxxxxxxxx",
        },
        timeout: 10000,
      };

      const responseSales = await axios(requestConfigSales);
      const salesDB = responseSales.data.results;

      const salesStage1 = [];
      const salesStage2 = [];
      const salesStage3 = [];
      const salesStage4 = [];

      if (salesDB.length > 0) {
        for (let index = 0; index < salesDB.length; index++) {
          const saleData = salesDB[index];

          switch (saleData.values.process) {
            case 1:
              salesStage1.push(JSON.parse(saleData.values.data));
              break;
            case 2:
              salesStage2.push(JSON.parse(saleData.values.data_formated));
              break;
            case 3:
              const dataFormated = [JSON.parse(saleData.values.data_formated)];
              salesStage3.push(
                dataFormated.map((sale) => sale.properties.numero_exp)[0]
              );
              break;
            case 4:
              const dataFormated2 = [JSON.parse(saleData.values.data_formated)];
              salesStage4.push(
                dataFormated2.map((sale) => sale.properties.numero_exp)[0]
              );
              break;
          }
        }
      }

      const config_sales = {
        method: "GET",
        url: `http://xxxxxxxxxxxxxxxxxxx/triario/sales`,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          Authorization: `Bearer ${token}`,
          Sandbox: true,
        },
        timeout: 10000,
      };

      const response_sales = await axios(config_sales);

      // const sales = response_sales.data;

      const sales = [
        {
          idUsuario: 1312981892,
          correo: "testcron@hotmail.com",
          telefono: "3107506984",
          identificacion: "1000352856",
          fechaMovimiento: "27/02/2024 00:00:00",
          nombreEvento: "Test",
          fechaEvento: "15/03/2024",
          fechaCompra: "27/02/2024",
          horaCompra: "08:00:00",
          tipoUsuario: "Comprador",
          origenDatos: "Secutix",
          promotor: "Tbl Live S.a.s",
          tema: "4. CONCIERTOS MOVISTAR ARENA",
          subTema: "Pop",
          tipoVenue: "MOVISTAR ARENA",
          nombreVenue: "Movistar Arena - Bogotá - Dg. 61c #26-36",
          ciudadEvento: "Bogota",
          departamentoEvento: "Bogota",
          paisEvento: "Colombia",
          canalCompra: "Web",
          numeroExpediente: 23234213,
          valorTickets: 800000,
          valorBfee: 64400,
          valorCompra: 424400,
          totalUnidades: 2,
          metodoPago: "Internet Visa Nacional",
          producto: "Evento",
        },
        {
          idUsuario: 2987612987298,
          correo: "testcron2@hotmail.com",
          telefono: "3107506984",
          identificacion: "1000352856",
          fechaMovimiento: "27/02/2024 00:00:00",
          nombreEvento: "Test 2",
          fechaEvento: "15/03/2024",
          fechaCompra: "27/02/2024",
          horaCompra: "08:00:00",
          tipoUsuario: "Comprador",
          origenDatos: "Secutix",
          promotor: "Tbl Live S.a.s",
          tema: "4. CONCIERTOS MOVISTAR ARENA",
          subTema: "Pop",
          tipoVenue: "MOVISTAR ARENA",
          nombreVenue: "Movistar Arena - Bogotá - Dg. 61c #26-36",
          ciudadEvento: "Bogota",
          departamentoEvento: "Bogota",
          paisEvento: "Colombia",
          canalCompra: "Web",
          numeroExpediente: 234234213,
          valorTickets: 360000,
          valorBfee: 64400,
          valorCompra: 424400,
          totalUnidades: 2,
          metodoPago: "Internet Visa Nacional",
          producto: "Evento",
        },
      ];

      const fusionS1 = [];
      const fusionS2 = [];
      const fusionS3 = [];
      const fusionS4 = [];

      for (const [index, sa] of salesStage1.entries()) {
        fusionS1.push({
          idUsuario: sa.idUsuario,
          correo: sa.correo,
          telefono: sa.telefono,
          identificacion: sa.identificacion,
          fechaMovimiento: sa.fechaMovimiento,
          nombreEvento: sa.nombreEvento,
          fechaEvento: sa.fechaEvento,
          fechaCompra: sa.fechaCompra,
          horaCompra: sa.horaCompra,
          tipoUsuario: sa.tipoUsuario,
          origenDatos: sa.origenDatos,
          promotor: sa.promotor,
          tema: sa.tema,
          subTema: sa.subTema,
          tipoVenue: sa.tipoVenue,
          nombreVenue: sa.nombreVenue,
          ciudadEvento: sa.ciudadEvento,
          departamentoEvento: sa.departamentoEvento,
          paisEvento: sa.paisEvento,
          canalCompra: sa.canalCompra,
          numeroExpediente: sa.numeroExpediente,
          valorTickets: sa.valorTickets,
          valorBfee: sa.valorBfee,
          valorCompra: sa.valorCompra,
          totalUnidades: sa.totalUnidades,
          metodoPago: sa.metodoPago,
          producto: sa.producto,
          retry: true,
        });
      }

      for (const [index, sa] of salesStage2.entries()) {
        fusionS2.push({
          properties: {
            tuboleta_user_id: sa.properties.tuboleta_user_id,
            email: sa.properties.email,
            fecha_de_movimiento: sa.properties.fecha_de_movimiento,
            nombre: sa.properties.nombre,
            fecha_hora_evento: sa.properties.fecha_hora_evento,
            fecha_de_compra: sa.properties.fecha_de_compra,
            hora_de_compra: sa.properties.hora_de_compra,
            promotor: sa.properties.promotor,
            tema: sa.properties.tema,
            subtema: sa.properties.subtema,
            tipo_venue: sa.properties.tipo_venue,
            venue: sa.properties.venue,
            ciudad_evento: sa.properties.ciudad_evento,
            canal_compra: sa.properties.canal_compra,
            numero_exp: sa.properties.numero_exp,
            valor: sa.properties.valor,
            booking_fee: sa.properties.booking_fee,
            valor_compra: sa.properties.valor_compra,
            numero_tickets: sa.properties.numero_tickets,
            metodo_de_pago: sa.properties.metodo_de_pago,
            familia_producto: sa.properties.familia_producto,
            integration: sa.properties.integration,
          },
        });
      }

      for (const [index, sa] of salesStage3.entries()) {
        fusionS3.push(sa);
      }

      for (const [index, sa] of salesStage4.entries()) {
        fusionS4.push(sa);
      }

      for (const [index, sa] of sales.entries()) {
        const found = fusionS1.find(
          (element) => element.numeroExpediente == sa.numeroExpediente
        );
        const found2 = fusionS2.find(
          (element) => element.properties.numero_exp == sa.numeroExpediente
        );
        const found3 = fusionS3.find(
          (element) => element == sa.numeroExpediente
        );
        const found4 = fusionS4.find(
          (element) => element == sa.numeroExpediente
        );

        if (!found?.numeroExpediente) {
          fusionS1.push({
            idUsuario: sa.idUsuario,
            correo: sa.correo,
            telefono: sa.telefono,
            identificacion: sa.identificacion,
            fechaMovimiento: sa.fechaMovimiento,
            nombreEvento: sa.nombreEvento,
            fechaEvento: sa.fechaEvento,
            fechaCompra: sa.fechaCompra,
            horaCompra: sa.horaCompra,
            tipoUsuario: sa.tipoUsuario,
            origenDatos: sa.origenDatos,
            promotor: sa.promotor,
            tema: sa.tema,
            subTema: sa.subTema,
            tipoVenue: sa.tipoVenue,
            nombreVenue: sa.nombreVenue,
            ciudadEvento: sa.ciudadEvento,
            departamentoEvento: sa.departamentoEvento,
            paisEvento: sa.paisEvento,
            canalCompra: sa.canalCompra,
            numeroExpediente: sa.numeroExpediente,
            valorTickets: sa.valorTickets,
            valorBfee: sa.valorBfee,
            valorCompra: sa.valorCompra,
            totalUnidades: sa.totalUnidades,
            metodoPago: sa.metodoPago,
            producto: sa.producto,
            retry: found2?.properties || found3 || found4 ? true : false,
          });
        }
      }

      const validatedSales = [];

      console.log(
        "--------------------- Verifying Sales ---------------------"
      );
      for (const sale of fusionS1) {
        const exist = await validateSale(sale);
        if (!sale.retry) {
          const data = {
            process: 1,
            exp_id: sale.numeroExpediente + "",
            sale_id: "",
            data: JSON.stringify(sale),
            data_formated: null,
            contact_association: 0,
            synced: exist ? 1 : 0,
          };

          const exist2 = await validateSaleHubDB(sale.numeroExpediente);

          if (exist2 == 0) {
            await insertSaleHubDB(data);
            // throw error;
          }
        }

        if (!exist) {
          validatedSales.push(await buildSale(sale));
          await updateSaleHubDB(await buildSale(sale));
          // throw error;
        }
        await delay(5000);
      }

      for (const [index, sa] of validatedSales.entries()) {
        const found = fusionS2.find(
          (element) => element.properties.numero_exp == sa.properties.numero_exp
        );

        if (!found?.properties) {
          fusionS2.push({
            properties: {
              tuboleta_user_id: sa.properties.tuboleta_user_id,
              email: sa.properties.email,
              fecha_de_movimiento: sa.properties.fecha_de_movimiento,
              nombre: sa.properties.nombre,
              fecha_hora_evento: sa.properties.fecha_hora_evento,
              fecha_de_compra: sa.properties.fecha_de_compra,
              hora_de_compra: sa.properties.hora_de_compra,
              promotor: sa.properties.promotor,
              tema: sa.properties.tema,
              subtema: sa.properties.subtema,
              tipo_venue: sa.properties.tipo_venue,
              venue: sa.properties.venue,
              ciudad_evento: sa.properties.ciudad_evento,
              canal_compra: sa.properties.canal_compra,
              numero_exp: sa.properties.numero_exp,
              valor: sa.properties.valor,
              booking_fee: sa.properties.booking_fee,
              valor_compra: sa.properties.valor_compra,
              numero_tickets: sa.properties.numero_tickets,
              metodo_de_pago: sa.properties.metodo_de_pago,
              familia_producto: sa.properties.familia_producto,
              integration: sa.properties.integration,
            },
          });
        }
      }

      const salesProperties = {
        inputs: validatedSales,
      };

      for (const [index, sa] of validatedSales.entries()) {
        const found = fusionS3.find((element) => element == sa);

        if (found) {
          fusionS3.push(sa);
        }
      }

      if (validatedSales.length > 0) {
        const config_create_sales = {
          method: "POST",
          url: `https://api.hubapi.com/crm/v3/objects/p45760644_historial_de_compras/batch/create`,
          headers: {
            "content-type": "application/json",
            Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
          },
          data: salesProperties,
          timeout: 10000,
        };

        let response = await axios(config_create_sales);

        const results = response.data.results;

        for (const sale of results) {
          const saleEmail = await getSale(sale.id);
          if (saleEmail !== 0) {
            const existContact = await validateContact(saleEmail);
            if (existContact !== 0) {
              const config_create_sales_assoc = {
                method: "PUT",
                url: `https://api.hubspot.com/crm/v3/objects/contacts/${existContact}/associations/p45760644_historial_de_compras/${sale.id}/18`,
                headers: {
                  "content-type": "application/json",
                  Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
                },
              };

              await axios(config_create_sales_assoc);
              await checkContactAssocSaleHubDB(
                sale.properties.numero_exp,
                sale.id
              );
              await syncSaleHubDB(sale.properties.numero_exp);
            }
          }
          await delay(5000);
        }
        console.log(
          "--------------------- Sales added successfully ---------------------"
        );
        //   context.log(`Sales added successfully!`);
      } else {
        console.log(
          "--------------------- No Sales to add ---------------------"
        );
        // context.log(`No Sales to add`);
      }

      if (fusionS3.length > 0) {
        for (let index = 0; index < fusionS3.length; index++) {
          const expId = fusionS3[index];
          const saleInfo = await getSaleByExp(expId);
          const saleEmail = await getSale(saleInfo.hs_object_id);
          if (saleEmail !== 0) {
            const existContact = await validateContact(saleEmail);
            if (existContact !== 0) {
              const config_create_sales_assoc = {
                method: "PUT",
                url: `https://api.hubspot.com/crm/v3/objects/contacts/${existContact}/associations/p45760644_historial_de_compras/${saleInfo.hs_object_id}/18`,
                headers: {
                  "content-type": "application/json",
                  Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
                },
              };

              await axios(config_create_sales_assoc);
              await checkContactAssocSaleHubDB(expId, saleInfo.hs_object_id);
              await syncSaleHubDB(expId);
            }
          }
          await delay(5000);
        }
      }

      for (const [index, sa] of validatedSales.entries()) {
        const found = fusionS4.find((element) => element == sa);

        if (found) {
          fusionS4.push(sa);
        }
      }

      if (fusionS4.length > 0) {
        for (let index = 0; index < fusionS4.length; index++) {
          const saleId = fusionS4[index];
          await syncSaleHubDB(saleId);
        }
      }
      console.log("--------------------- Syncing Carts ---------------------");
      await delay(5000);

      const requestConfigCarts = {
        method: "GET",
        url: "https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows?synced=false",
        headers: {
          Authorization: "Bearer xxxxxxxxxxxxxxxxxxx",
        },
        timeout: 10000,
      };

      const responseCarts = await axios(requestConfigCarts);
      const cartsDB = responseCarts.data.results;

      const cartsStage1 = [];
      const cartsStage2 = [];
      const cartsStage3 = [];
      const cartsStage4 = [];

      if (cartsDB.length > 0) {
        for (let index = 0; index < cartsDB.length; index++) {
          const cartData = cartsDB[index];

          switch (cartData.values.process) {
            case 1:
              cartsStage1.push(JSON.parse(cartData.values.data));
              break;
            case 2:
              cartsStage2.push(JSON.parse(cartData.values.data_formated));
              break;
            case 3:
              const dataFormated = [JSON.parse(cartData.values.data_formated)];
              cartsStage3.push(
                dataFormated.map((cart) => cart.properties.numero_exp)[0]
              );
              break;
            case 4:
              const dataFormated2 = [JSON.parse(cartData.values.data_formated)];
              cartsStage4.push(
                dataFormated2.map((cart) => cart.properties.numero_exp)[0]
              );
              break;
          }
        }
      }

      const config_carts = {
        method: "GET",
        url: `http://xxxxxxxxxxxxxxxxxxx/triario/carts`,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          Authorization: `Bearer ${token}`,
          Sandbox: true,
        },
        timeout: 10000,
      };

      const response_carts = await axios(config_carts);

      // const carts = response_carts.data;

      const carts = [
        {
          idUsuario: 1312981892,
          correo: "testcron@hotmail.com",
          telefono: "123456789",
          identificacion: "ID001",
          fechaMovimiento: "16/05/2024 00:00:00",
          nombreEvento: "Evento GEN",
          fechaEvento: "12/06/2024",
          tipoUsuario: "Tipo1",
          origenDatos: "Secutix",
          promotor: "Promotor1",
          tema: "Conciertos",
          subTema: "Festival",
          tipoVenue: "Estadio",
          nombreVenue: "NombreVenue1",
          ciudadEvento: "Cali",
          departamentoEvento: "VALLE",
          paisEvento: "Colombia",
          canalCompra: "Web",
          numeroExpediente: 123,
          valorTickets: 100,
          valorBfee: 20,
          valorCompra: 120,
          totalUnidades: 3,
          metodoPago: "Pago1",
          producto: "Bono_Regalo",
          dealname: "testcron-123",
          dealstage: 89158472,
        },
        {
          idUsuario: 2987612987298,
          correo: "testcron2@hotmail.com",
          telefono: "123456789",
          identificacion: "ID001",
          fechaMovimiento: "16/05/2024 00:00:00",
          nombreEvento: "Evento GEN",
          fechaEvento: "12/06/2024",
          tipoUsuario: "Tipo1",
          origenDatos: "Secutix",
          promotor: "Promotor1",
          tema: "Conciertos",
          subTema: "Festival",
          tipoVenue: "Estadio",
          nombreVenue: "NombreVenue1",
          ciudadEvento: "Cali",
          departamentoEvento: "VALLE",
          paisEvento: "Colombia",
          canalCompra: "Web",
          numeroExpediente: 456,
          valorTickets: 100,
          valorBfee: 20,
          valorCompra: 120,
          totalUnidades: 3,
          metodoPago: "Pago1",
          producto: "Bono_Regalo",
          dealname: "testcron2-456",
          dealstage: 89158472,
        },
      ];

      const fusionC1 = [];
      const fusionC2 = [];
      const fusionC3 = [];
      const fusionC4 = [];

      for (const [index, ca] of cartsStage1.entries()) {
        fusionC1.push({
          idUsuario: ca.idUsuario,
          correo: ca.correo,
          telefono: ca.telefono,
          identificacion: ca.identificacion,
          fechaMovimiento: ca.fechaMovimiento,
          nombreEvento: ca.nombreEvento,
          fechaEvento: ca.fechaEvento,
          tipoUsuario: ca.tipoUsuario,
          origenDatos: ca.origenDatos,
          promotor: ca.promotor,
          tema: ca.tema,
          subTema: ca.subTema,
          tipoVenue: ca.tipoVenue,
          nombreVenue: ca.nombreVenue,
          ciudadEvento: ca.ciudadEvento,
          departamentoEvento: ca.departamentoEvento,
          paisEvento: ca.paisEvento,
          canalCompra: ca.canalCompra,
          numeroExpediente: ca.numeroExpediente,
          valorTickets: ca.valorTickets,
          valorBfee: ca.valorBfee,
          valorCompra: ca.valorCompra,
          totalUnidades: ca.totalUnidades,
          metodoPago: ca.metodoPago,
          producto: ca.producto,
          dealname: ca.dealname,
          dealstage: ca.dealstage,
          retry: true,
        });
      }

      for (const [index, ca] of cartsStage2.entries()) {
        fusionC2.push({
          properties: {
            tuboleta_user_id: ca.properties.tuboleta_user_id,
            email: ca.properties.email,
            telefono: ca.properties.telefono,
            documento_identificacion: ca.properties.documento_identificacion,
            fecha_movimiento: ca.properties.fecha_movimiento,
            nombre_evento: ca.properties.nombre_evento,
            fecha_evento: ca.properties.fecha_evento,
            tipo_usuario: ca.properties.tipo_usuario,
            origen_de_datos: ca.properties.origen_de_datos,
            promotor: ca.properties.promotor,
            tema: ca.properties.tema,
            subtema: ca.properties.subtema,
            tipo_venue: ca.properties.tipo_venue,
            nombre_venue: ca.properties.nombre_venue,
            ciudad_evento: ca.properties.ciudad_evento,
            departamento_evento: ca.properties.departamento_evento,
            pais_evento: ca.properties.pais_evento,
            canal_compra: ca.properties.canal_compra,
            numero_exp: ca.properties.numero_exp,
            valor_tickets: ca.properties.valor_tickets,
            valor_bfee: ca.properties.valor_bfee,
            valor_compra: ca.properties.valor_compra,
            total_unidades: ca.properties.total_unidades,
            metodo_pago: ca.properties.metodo_pago,
            producto: ca.properties.producto,
            dealname: ca.properties.dealname,
            dealstage: ca.properties.dealstage,
            integration: ca.properties.integration,
          },
        });
      }

      for (const [index, ca] of cartsStage3.entries()) {
        fusionC3.push(ca);
      }

      for (const [index, ca] of cartsStage4.entries()) {
        fusionC4.push(ca);
      }

      for (const [index, ca] of carts.entries()) {
        const found = fusionC1.find(
          (element) => element.numeroExpediente == ca.numeroExpediente
        );
        const found2 = fusionC2.find(
          (element) => element.properties.numero_exp == ca.numeroExpediente
        );
        const found3 = fusionC3.find(
          (element) => element == ca.numeroExpediente
        );
        const found4 = fusionC4.find(
          (element) => element == ca.numeroExpediente
        );

        if (!found?.numeroExpediente) {
          fusionC1.push({
            idUsuario: ca.idUsuario,
            correo: ca.correo,
            telefono: ca.telefono,
            identificacion: ca.identificacion,
            fechaMovimiento: ca.fechaMovimiento,
            nombreEvento: ca.nombreEvento,
            fechaEvento: ca.fechaEvento,
            tipoUsuario: ca.tipoUsuario,
            origenDatos: ca.origenDatos,
            promotor: ca.promotor,
            tema: ca.tema,
            subTema: ca.subTema,
            tipoVenue: ca.tipoVenue,
            nombreVenue: ca.nombreVenue,
            ciudadEvento: ca.ciudadEvento,
            departamentoEvento: ca.departamentoEvento,
            paisEvento: ca.paisEvento,
            canalCompra: ca.canalCompra,
            numeroExpediente: ca.numeroExpediente,
            valorTickets: ca.valorTickets,
            valorBfee: ca.valorBfee,
            valorCompra: ca.valorCompra,
            totalUnidades: ca.totalUnidades,
            metodoPago: ca.metodoPago,
            producto: ca.producto,
            dealname: ca.dealname,
            dealstage: ca.dealstage,
            retry: found2?.properties || found3 || found4 ? true : false,
          });
        }
      }

      const validatedCarts = [];

      console.log(
        "--------------------- Verifying Carts ---------------------"
      );

      for (const cart of fusionC1) {
        const exist = await validateCart(cart);

        if (!cart.retry) {
          const data = {
            process: 1,
            exp_id: cart.numeroExpediente + "",
            cart_id: "",
            data: JSON.stringify(cart),
            data_formated: null,
            contact_association: 0,
            synced: exist ? 1 : 0,
          };

          const exist2 = await validateCartHubDB(cart.numeroExpediente);

          if (exist2 == 0) {
            await insertCartHubDB(data);
            // throw error
          }
        }

        if (!exist) {
          validatedCarts.push(await buildCart(cart));
          await updateCartHubDB(await buildCart(cart));
          // throw error
        }
        await delay(5000);
      }

      for (const [index, ca] of validatedCarts.entries()) {
        const found = fusionC2.find(
          (element) => element.properties.numero_exp == ca.properties.numero_exp
        );

        if (!found?.properties) {
          fusionC2.push({
            properties: {
              tuboleta_user_id: ca.properties.tuboleta_user_id,
              email: ca.properties.email,
              telefono: ca.properties.telefono,
              documento_identificacion: ca.properties.documento_identificacion,
              fecha_movimiento: ca.properties.fecha_movimiento,
              nombre_evento: ca.properties.nombre_evento,
              fecha_evento: ca.properties.fecha_evento,
              tipo_usuario: ca.properties.tipo_usuario,
              origen_de_datos: ca.properties.origen_de_datos,
              promotor: ca.properties.promotor,
              tema: ca.properties.tema,
              subtema: ca.properties.subtema,
              tipo_venue: ca.properties.tipo_venue,
              nombre_venue: ca.properties.nombre_venue,
              ciudad_evento: ca.properties.ciudad_evento,
              departamento_evento: ca.properties.departamento_evento,
              pais_evento: ca.properties.pais_evento,
              canal_compra: ca.properties.canal_compra,
              numero_exp: ca.properties.numero_exp,
              valor_tickets: ca.properties.valor_tickets,
              valor_bfee: ca.properties.valor_bfee,
              valor_compra: ca.properties.valor_compra,
              total_unidades: ca.properties.total_unidades,
              metodo_pago: ca.properties.metodo_pago,
              producto: ca.properties.producto,
              dealname: ca.properties.dealname,
              dealstage: ca.properties.dealstage,
              integration: ca.properties.integration,
            },
          });
        }
      }

      const cartsProperties = {
        inputs: validatedCarts,
      };

      for (const [index, ca] of validatedCarts.entries()) {
        const found = fusionC3.find((element) => element == ca);

        if (found) {
          fusionC3.push(ca);
        }
      }

      if (validatedCarts.length > 0) {
        const config_create_carts = {
          method: "POST",
          url: `https://api.hubapi.com/crm/v3/objects/deals/batch/create`,
          headers: {
            "content-type": "application/json",
            Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
          },
          data: cartsProperties,
          timeout: 10000,
        };

        let response = await axios(config_create_carts);

        const resultsC = response.data.results;

        for (const cart of resultsC) {
          const cartEmail = await getCart(cart.id);
          if (cartEmail !== 0) {
            const existContact = await validateContact(cartEmail);
            if (existContact !== 0) {
              const config_create_carts_assoc = {
                method: "PUT",
                url: `https://api.hubspot.com/crm/v3/objects/contacts/${existContact}/associations/deals/${cart.id}/4`,
                headers: {
                  "content-type": "application/json",
                  Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
                },
              };

              await axios(config_create_carts_assoc);
              await checkContactAssocCartHubDB(
                cart.properties.numero_exp,
                cart.id
              );
              await syncCartHubDB(cart.properties.numero_exp);
            }
          }
          await delay(5000);
        }
        console.log(
          "--------------------- Carts added successfully ---------------------"
        );
        //   context.log(`Carts added successfully!`);
      } else {
        console.log(
          "--------------------- No Carts to add ---------------------"
        );
        //   context.log(`No Carts to add`);
      }

      if (fusionC3.length > 0) {
        for (let index = 0; index < fusionC3.length; index++) {
          const expId = fusionC3[index];
          const cartInfo = await getCartByExp(expId);
          const cartEmail = await getCart(cartInfo.hs_object_id);
          if (cartEmail !== 0) {
            const existContact = await validateContact(cartEmail);
            if (existContact !== 0) {
              const config_create_carts_assoc = {
                method: "PUT",
                url: `https://api.hubspot.com/crm/v3/objects/contacts/${existContact}/associations/p45760644_historial_de_compras/${cartInfo.hs_object_id}/18`,
                headers: {
                  "content-type": "application/json",
                  Authorization: `Bearer xxxxxxxxxxxxxxxxxxx`,
                },
              };

              await axios(config_create_carts_assoc);
              await checkContactAssocCartHubDB(expId, cartInfo.hs_object_id);
              await syncCartHubDB(expId);
            }
          }
          await delay(5000);
        }
      }

      for (const [index, ca] of validatedCarts.entries()) {
        const found = fusionC4.find((element) => element == ca);

        if (found) {
          fusionC4.push(ca);
        }
      }

      if (fusionC4.length > 0) {
        for (let index = 0; index < fusionC4.length; index++) {
          const cartId = fusionC4[index];
          await syncCartHubDB(cartId);
        }
      }

      return { body: `Objects Created/Updated Successfully!` };
    } catch (error) {
      context.error(error);
    }
  },
});

const validateContactHubDB = async (userId) => {
  const requestConfig = {
      method: 'GET',
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20720399/rows?user_id__eq=${userId}`,
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      timeout: 10000
  }

  const response = await axios(requestConfig);
  const jsonResponse = response.data;
  if (jsonResponse.total > 0) {
      return jsonResponse.results[0];
  }

  return 0;
}

const InsertUserHubDB = async (data) => {

  const requestConfig = {
      method: 'POST',
      url: 'https://api.hubapi.com/cms/v3/hubdb/tables/20720399/rows',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          'content-type': 'application/json'
      },

      data: JSON.stringify({
          "values": data
      })
  }

  const insertRequest = await axios(requestConfig);
  if (insertRequest.status == 201 ) {
      await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20720399/publish`, {},
          {
              headers:{
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
              }
          }
      );

  }else{
      throw new Error('Error al insertar en HDB');
  }
}

const UpdateUserHubDB = async (user) => {

  const hdbQuoteObject = await validateContactHubDB(user.properties.tuboleta_user_id);
  
  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }
  // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
  let hdbQuoteData = hdbQuoteObject.values;

  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20720399/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
     
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20720399/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 2,
      user_id: hdbQuoteData.user_id,
      data: hdbQuoteData.data,
      data_formated: JSON.stringify(user),
      synced: hdbQuoteData.synced,
  };

  await InsertUserHubDB(resultData);
}

const SyncUserHubDB = async (users) => {
  for (let index = 0; index < users.length; index++) {
      const userId = users[index];
      const hdbQuoteObject = await validateContactHubDB(userId);

      if (hdbQuoteObject == null) {
          return res.status(500).json({
              ok: false,
              msg: 'No se encontró el registro'
          })
      }
      // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
      let hdbQuoteData = hdbQuoteObject.values;

      const requestConfig2 = {
          url: `https://api.hubapi.com/cms/v3/hubdb/tables/20720399/rows/${hdbQuoteObject.id}/draft`,
          method: 'DELETE',
          headers: {
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          },
      
      }
      await axios(requestConfig2);

      await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20720399/publish`, {},
          {
              headers:{
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
              }
          }
      );

      let resultData = {
          process: 3,
          user_id: hdbQuoteData.user_id,
          data: hdbQuoteData.data,
          data_formated: hdbQuoteData.data_formated,
          synced: 1,
      };

      await InsertUserHubDB(resultData);
  }
}

const delay = (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const validateUser = async (user) => {
  const config_validate_contact = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/contacts/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "email",
              "tuboleta_user_id"
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "email",
                          "value": user.correo,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_contact = await axios(config_validate_contact);

  if (response_validate_contact.data.total >= 1 && !response_validate_contact.data.results[0]?.properties.tuboleta_id_updated) {
      const config_update_contact = {
          method: 'PATCH',
          url: `https://api.hubapi.com/crm/v3/objects/contacts/${response_validate_contact.data.results[0].properties.hs_object_id}`,
          headers: {
              'accept': 'application/json',
              'content-type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          },
          data: {
              "properties": {
                  "tuboleta_id_updated": true,
                  "tuboleta_user_id": user.idUsuario,
              },
          },
          timeout: 10000
      }

      await axios(config_update_contact);
  }

  return response_validate_contact.data.total >= 1;
}

const capitalizeFirstLetter = (string) => {
  return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
}

const buildContact = (user) => {
  return {
      "properties": {
          "tuboleta_user_id": user.idUsuario,
          "tuboleta_id_updated": true,
          "email": user.correo,
          "firstname": user.nombre,
          "lastname": user.apellido,
          "phone": user.telefono,
          "documento_identificacion": user.identificacion,
          "fecha_movimiento": user.fechaMovimiento,
          "tipo_documento_identificacion": (user.tipoIdentificacion == "CC")? "Cédula Ciudadania" : "",
          "genero": capitalizeFirstLetter(user.genero),
          "tipo_usuario": user.tipoUsuario,
          "date_of_birth": user.fechaNacimiento,
          "city": user.ciudad,
          "state": user.departamento,
          "country": user.pais.toUpperCase(),
          "address": user.direccion,
          "tratamiento_datos": user.tratamientoDatos,
          "integration": true
      }
  };
}

const validateSaleHubDB = async (saleId) => {
  const requestConfig = {
      method: 'GET',
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows?exp_id__eq=${saleId}`,
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      timeout: 10000
  }

  const response = await axios(requestConfig);
  const jsonResponse = response.data;
  if (jsonResponse.total > 0) {
      return jsonResponse.results[0];
  }

  return 0;
}

const insertSaleHubDB = async (data) => {

  const requestConfig = {
      method: 'POST',
      url: 'https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          'content-type': 'application/json'
      },

      data: JSON.stringify({
          "values": data
      })
  }

  const insertRequest = await axios(requestConfig);
  if (insertRequest.status == 201 ) {
      await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20929462/publish`, {},
          {
              headers:{
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
              }
          }
      );

  }else{
      throw new Error('Error al insertar en HDB');
  }
}

const updateSaleHubDB = async (sale) => {
  const hdbQuoteObject = await validateSaleHubDB(sale.properties.numero_exp);
  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }
  // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
  let hdbQuoteData = hdbQuoteObject.values;
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
     
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20929462/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 2,
      exp_id: hdbQuoteData.exp_id,
      sale_id: hdbQuoteData.sale_id,
      data: hdbQuoteData.data,
      data_formated: JSON.stringify(sale),
      contact_association: hdbQuoteData.contact_association,
      synced: hdbQuoteData.synced,
  };

  await insertSaleHubDB(resultData);
}

const checkContactAssocSaleHubDB = async (expId, saleId) => {
  
  const hdbQuoteObject = await validateSaleHubDB(expId);

  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }

  let hdbQuoteData = hdbQuoteObject.values;
  
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
  
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20929462/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 3,
      exp_id: hdbQuoteData.exp_id,
      sale_id: saleId,
      data: hdbQuoteData.data,
      data_formated: hdbQuoteData.data_formated,
      contact_association: 1,
      synced: hdbQuoteData.synced,
  };

  await insertSaleHubDB(resultData);
  
}

const syncSaleHubDB = async (saleId) => {
  const hdbQuoteObject = await validateSaleHubDB(saleId);

  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }
  // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
  let hdbQuoteData = hdbQuoteObject.values;
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/20929462/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
  
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/20929462/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 4,
      exp_id: hdbQuoteData.exp_id,
      sale_id: hdbQuoteData.sale_id,
      data: hdbQuoteData.data,
      data_formated: hdbQuoteData.data_formated,
      contact_association: hdbQuoteData.contact_association,
      synced: 1,
  };

  await insertSaleHubDB(resultData);
}

const validateSale = async (sale) => {
  const config_validate_sale = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/p45760644_historial_de_compras/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "email",
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "numero_exp",
                          "value": sale.numeroExpediente,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_sale = await axios(config_validate_sale);


  return response_validate_sale.data.total >= 1;
}

const getSaleByExp = async (sale) => {
  const config_validate_sale = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/p45760644_historial_de_compras/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "email",
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "numero_exp",
                          "value": sale,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_sale = await axios(config_validate_sale);
  
  let saleInfo = 0;
  
  if (response_validate_sale.data.total >= 1) {
      saleInfo = response_validate_sale.data.results[0].properties
  }

  return saleInfo;


}

const getSale = async (saleId) => {
  const config_validate_sale = {
      method: 'GET',
      url: `https://api.hubapi.com/crm/v3/objects/p45760644_historial_de_compras/${saleId}?properties=email`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      }
  }

  const response_validate_sale = await axios(config_validate_sale);

  let saleEmail = 0;

  if (Object.keys(response_validate_sale.data).length > 0) {
      saleEmail = response_validate_sale.data.properties.email
  }

  return saleEmail;
}

const validateContact = async (email) => {
  const config_validate_contact = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/contacts/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "hs_object_id"
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "email",
                          "value": email,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_contact = await axios(config_validate_contact);

  let contactId = 0;

  if (response_validate_contact.data.total >= 1) {
      contactId = response_validate_contact.data.results[0].properties.hs_object_id
  }

  return contactId;
}

const buildSale = async (sale) => {
  return {
      "properties": {
          "tuboleta_user_id":  sale.idUsuario,
          "email":  sale.correo,
          "fecha_de_movimiento":  sale.fechaMovimiento,
          "nombre":  sale.nombreEvento,
          "fecha_hora_evento":  sale.fechaEvento,
          "fecha_de_compra":  sale.fechaCompra,
          "hora_de_compra":  sale.horaCompra,
          "promotor":  sale.promotor,
          "tema":  sale.tema,
          "subtema":  sale.subTema,
          "tipo_venue":  sale.tipoVenue,
          "venue":  sale.nombreVenue,
          "ciudad_evento": await validateCity(sale.ciudadEvento),
          "canal_compra":  sale.canalCompra,
          "numero_exp":  sale.numeroExpediente,
          "valor":  sale.valorTickets,
          "booking_fee":  sale.valorBfee,
          "valor_compra":  sale.valorCompra,
          "numero_tickets":  sale.totalUnidades,
          "metodo_de_pago":  sale.metodoPago,
          "familia_producto":  sale.producto,
          "integration": true
      }
  };
}

const validateCity = (cityInput) => {
  let cityOutPut = cityInput;

  switch (cityInput) {
      case "Bogota":
          cityOutPut = "Bogotá"
          break;
      case "Medellin":
          cityOutPut = "Medellín"
          break;
      case "Cucuta":
          cityOutPut = "Cúcuta"
          break;
      case "Ibague":
          cityOutPut = "Ibagué"
          break;
      case "Cucuta":
          cityOutPut = "Cúcuta"
          break;
      case "Monteria":
          cityOutPut = "Montería"
          break;
      case "Popayan":
          cityOutPut = "Popayán"
          break;
      case "Itagui":
      case "Itagüi":
          cityOutPut = "Itagüí"
          break;
      case "Tulua":
          cityOutPut = "Tuluá"
          break;
  }
  
  return cityOutPut;
}

const validateCartHubDB = async (cartId) => {
  const requestConfig = {
      method: 'GET',
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows?exp_id__eq=${cartId}`,
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      timeout: 10000
  }

  const response = await axios(requestConfig);
  const jsonResponse = response.data;
  if (jsonResponse.total > 0) {
      return jsonResponse.results[0];
  }

  return 0;
}

const insertCartHubDB = async (data) => {

  const requestConfig = {
      method: 'POST',
      url: 'https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          'content-type': 'application/json'
      },

      data: JSON.stringify({
          "values": data
      })
  }

  const insertRequest = await axios(requestConfig);
  if (insertRequest.status == 201 ) {
      await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/21090178/publish`, {},
          {
              headers:{
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
              }
          }
      );

  }else{
      throw new Error('Error al insertar en HDB');
  }
}

const updateCartHubDB = async (cart) => {
  const hdbQuoteObject = await validateCartHubDB(cart.properties.numero_exp);
  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }
  // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
  let hdbQuoteData = hdbQuoteObject.values;
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
     
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/21090178/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 2,
      exp_id: hdbQuoteData.exp_id,
      cart_id: hdbQuoteData.cart_id,
      data: hdbQuoteData.data,
      data_formated: JSON.stringify(cart),
      contact_association: hdbQuoteData.contact_association,
      synced: hdbQuoteData.synced,
  };

  await insertCartHubDB(resultData);
}

const checkContactAssocCartHubDB = async (expId, cartId) => {
  
  const hdbQuoteObject = await validateCartHubDB(expId);

  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }

  let hdbQuoteData = hdbQuoteObject.values;
  
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
  
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/21090178/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 3,
      exp_id: hdbQuoteData.exp_id,
      cart_id: cartId,
      data: hdbQuoteData.data,
      data_formated: hdbQuoteData.data_formated,
      contact_association: 1,
      synced: hdbQuoteData.synced,
  };

  await insertCartHubDB(resultData);
  
}

const syncCartHubDB = async (cartId) => {
  const hdbQuoteObject = await validateCartHubDB(cartId);

  if (hdbQuoteObject == null) {
      return res.status(500).json({
          ok: false,
          msg: 'No se encontró el registro'
      })
  }
  // const hdbQuoteData = getHDBQuoteData(hdbQuoteObject);
  let hdbQuoteData = hdbQuoteObject.values;
  const requestConfig2 = {
      url: `https://api.hubapi.com/cms/v3/hubdb/tables/21090178/rows/${hdbQuoteObject.id}/draft`,
      method: 'DELETE',
      headers: {
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
  
  }
  await axios(requestConfig2);

  await axios.put(`https://api.hubapi.com/hubdb/api/v2/tables/21090178/publish`, {},
      {
          headers:{
              'Content-Type': 'application/json',
              'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
          }
      }
  );

  let resultData = {
      process: 4,
      exp_id: hdbQuoteData.exp_id,
      cart_id: hdbQuoteData.cart_id,
      data: hdbQuoteData.data,
      data_formated: hdbQuoteData.data_formated,
      contact_association: hdbQuoteData.contact_association,
      synced: 1,
  };

  await insertCartHubDB(resultData);
}

const buildCart = async (cart) => {
  return {
      "properties": {
          "tuboleta_user_id": cart.idUsuario,
          "email": cart.correo,
          "telefono": cart.telefono,
          "documento_identificacion": cart.identificacion,
          "fecha_movimiento": cart.fechaMovimiento,
          "nombre_evento": cart.nombreEvento,
          "fecha_evento": cart.fechaEvento,
          "tipo_usuario": cart.tipoUsuario,
          "origen_de_datos": cart.origenDatos,
          "promotor": cart.promotor,
          "tema": cart.tema,
          "subtema": cart.subTema,
          "tipo_venue": cart.tipoVenue,
          "nombre_venue": cart.nombreVenue,
          "ciudad_evento": await validateCity(cart.ciudadEvento),
          "departamento_evento": cart.departamentoEvento,
          "pais_evento": cart.paisEvento,
          "canal_compra": cart.canalCompra,
          "numero_exp": cart.numeroExpediente,
          "valor_tickets": cart.valorTickets,
          "valor_bfee": cart.valorBfee,
          "valor_compra": cart.valorCompra,
          "total_unidades": cart.totalUnidades,
          "metodo_pago": cart.metodoPago,
          "producto": cart.producto,
          "dealname": cart.dealname,
          "dealstage": cart.dealstage,
          "integration": true
      }
  };
}

const getCart = async (cartsId) => {
  const config_validate_cart = {
      method: 'GET',
      url: `https://api.hubapi.com/crm/v3/objects/deals/${cartsId}?properties=email`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      }
  }

  const response_validate_cart = await axios(config_validate_cart);

  let cartEmail = 0;

  if (Object.keys(response_validate_cart.data).length > 0) {
      cartEmail = response_validate_cart.data.properties.email
  }

  return cartEmail;
}

const validateCart = async (cart) => {

  const config_validate_cart = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/deals/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "email",
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "numero_exp",
                          "value": cart.numeroExpediente,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_cart = await axios(config_validate_cart);

  return response_validate_cart.data.total >= 1;
}

const getCartByExp = async (cart) => {
  const config_validate_cart = {
      method: 'POST',
      url: `https://api.hubapi.com/crm/v3/objects/deals/search`,
      headers: {
          'accept': 'application/json',
          'content-type': 'application/json',
          'Authorization': `Bearer xxxxxxxxxxxxxxxxxxx`,
      },
      data: {
          "properties": [
              "email",
          ],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "numero_exp",
                          "value": cart,
                          "operator": "EQ"
                      }
                  ]
              }
          ]
      },
      timeout: 10000
  }

  const response_validate_cart = await axios(config_validate_cart);
  
  let cartInfo = 0;
  
  if (response_validate_cart.data.total >= 1) {
      cartInfo = response_validate_cart.data.results[0].properties
  }

  return cartInfo;
}
